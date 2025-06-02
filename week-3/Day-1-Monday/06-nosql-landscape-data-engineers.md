# NoSQL Landscape for Data Engineers



## Introduction

Every time you've stored complex JSON from an API in your relational database, you've encountered the exact problem NoSQL databases were designed to solve. 

As your applications scale, the limitations of traditional table structures become increasingly apparent—whether it's performance bottlenecks from complex joins, the rigidity of fixed schemas, or the mismatch between object-oriented code and relational storage. Modern data pipelines rarely operate within a single database paradigm; instead, they continuously move data between SQL and NoSQL systems to leverage the strengths of each. 

This lesson equips you with practical knowledge about the NoSQL landscape, helping you make informed architectural decisions, understand the trade-offs in different database paradigms, and prepare for the real-world challenges of working with diverse data sources.



## Learning Outcomes

By the end of this lesson, you will be able to:

1. Differentiate between NoSQL and relational database paradigms by analyzing their consistency models, scaling approaches, and data modeling flexibility.
2. Evaluate the four main NoSQL database types (Document, Key-Value, Column-Family, and Graph) based on their data models, real-world applications, and performance characteristics.
3. Assess NoSQL technologies for data engineering workflows by examining ETL integration requirements, data transformation needs, and implementation trade-offs in modern architectures.



## NoSQL vs Relational Database Paradigms

### Data Consistency Models: ACID vs BASE

**Focus:** Understanding fundamental differences in data consistency guarantees

In your MySQL experience, you've relied on transactions to ensure data integrity. When you transfer money between accounts, either both operations succeed or neither does. This is the ACID model that relational databases provide.

#### ACID Properties in Relational Databases

ACID stands for:

- **Atomicity**: Transactions are all-or-nothing. If any part fails, the entire transaction is rolled back.
- **Consistency**: Data must satisfy all defined rules after a transaction completes.
- **Isolation**: Concurrent transactions don't interfere with each other.
- **Durability**: Once committed, changes survive even system failures.

In your Java applications with MySQL, you've likely used code like this:

```java
try {
    connection.setAutoCommit(false);
    // Multiple SQL operations
    connection.commit();
} catch (Exception e) {
    connection.rollback();
}
```

This ensures that related operations succeed or fail together—critical for applications like banking or inventory management.

#### BASE Properties in NoSQL

NoSQL databases often follow a different model called BASE:

- **Basically Available**: The system guarantees availability
- **Soft state**: The system may change over time, even without input
- **Eventually consistent**: The system will become consistent over time

Unlike the immediate consistency of ACID, BASE systems prioritize availability and partition tolerance. They might temporarily show different values to different users, but will eventually converge.

This is like collaborative document editing: multiple people can make changes, and everyone eventually sees the same document, but there might be moments when different users see different versions.

#### The CAP Theorem

The CAP theorem states that distributed databases can guarantee only two of these three properties:

- **Consistency**: All nodes see the same data at the same time
- **Availability**: The system remains operational even if nodes fail
- **Partition tolerance**: The system continues to function despite network partitions

Traditional relational databases typically prioritize consistency and availability, sacrificing partition tolerance. Many NoSQL databases choose availability and partition tolerance, accepting eventual consistency.

When interviewing for data engineering positions, explaining these trade-offs demonstrates your understanding of fundamental database design principles and helps you communicate why certain databases are chosen for specific use cases.



### Scaling Approaches and Architecture

**Focus:** How scaling differs between paradigms

As your Java applications grow, you've likely encountered performance bottlenecks with MySQL. Perhaps queries slowed down as tables grew, or connection limits became a problem during traffic spikes.

#### Vertical Scaling in Relational Databases

Relational databases traditionally scale vertically:

- **Add more resources** to a single server (CPU, RAM, faster storage)
- **Optimize queries and indexes** to improve performance
- **Implement caching layers** to reduce database load

This approach works well up to a point, but eventually hits physical and cost limitations. You can only add so much RAM to one server, and downtime during upgrades affects availability.

#### Horizontal Scaling in NoSQL

NoSQL databases are designed for horizontal scaling:

- **Distribute data across multiple servers** (sharding)
- **Add more nodes** to handle increased load
- **Automatically rebalance** data as the cluster grows

This approach offers nearly linear scaling: doubling the nodes roughly doubles capacity. However, it introduces complexity in data consistency and distribution.

#### Read vs. Write Scaling

Different applications have different read/write ratios:

- **Read-heavy applications** (like content sites) benefit from read replicas and caching
- **Write-heavy applications** (like logging systems) need efficient write distribution

NoSQL databases often allow you to optimize for specific patterns:

- MongoDB can scale reads through replica sets
- Cassandra excels at write scaling through its distributed architecture
- Redis provides in-memory performance for high-throughput scenarios

Understanding these scaling approaches helps you design data architectures that grow with your application needs. In interviews, discussing how you'd approach scaling challenges demonstrates practical thinking that employers value.



### Data Modeling Flexibility

**Focus:** Schema design differences and implications

In your Java applications with MySQL, you've designed normalized database schemas with strictly defined tables, columns, and relationships. Every record in a table follows the same structure.

#### Fixed Schemas vs. Schema Flexibility

Relational databases enforce fixed schemas:

- Structure must be defined before data is inserted
- Schema changes require ALTER TABLE operations
- All records in a table have identical structure

NoSQL databases offer various levels of schema flexibility:

- **Schema-less**: No predefined structure (pure key-value stores)
- **Schema-flexible**: Basic structure with optional fields (document databases)
- **Schema-on-read**: Structure applied when data is queried, not when stored

This is similar to the difference between strongly-typed languages (like Java) and dynamically-typed languages (like JavaScript). Both approaches have advantages in different scenarios.

#### Normalization vs. Denormalization

In relational design, you normalize data to reduce redundancy:

- Split data into multiple tables
- Use foreign keys to create relationships
- Join tables when querying related data

NoSQL often encourages denormalization:

- Duplicate data to avoid complex joins
- Store related data together in a single document
- Optimize for query patterns rather than storage efficiency

For example, in a relational database, you might have separate tables for orders and customers:

```sql
SELECT o.order_id, o.amount, c.name 
FROM orders o
JOIN customers c ON o.customer_id = c.id;
```

In a document database like MongoDB, you might embed customer data directly in each order document:

```json
{
  "order_id": "12345",
  "amount": 99.99,
  "customer": {
    "name": "Jane Smith",
    "email": "jane@example.com"
  }
}
```

#### Impact on Development and Maintenance

These differences significantly affect application development:

- **Schema evolution**: NoSQL typically allows adding fields without downtime
- **Development speed**: Flexible schemas can accelerate early development
- **Data integrity**: Relational constraints provide stronger guarantees
- **Query complexity**: Joins are simpler in relational databases

When interviewing for data engineering roles, explaining these trade-offs demonstrates your ability to make informed architectural decisions based on business requirements rather than just technical preferences.



## NoSQL Database Types and Use Cases

### Document Databases

**Focus:** Understanding document database concepts and applications

If you've worked with JSON in your React applications, you're already familiar with the concept of documents—self-contained data objects with nested structures. Document databases store and query these objects natively.

#### Document Structure and Flexibility

Document databases store data as JSON-like documents:

- **Flexible schema**: Each document can have different fields
- **Nested data**: Documents can contain arrays and embedded objects
- **Self-contained**: A document often contains all related information

This structure maps naturally to objects in your Java or JavaScript code:

```json
{
  "_id": "5f8a7b6c5e4d3c2b1a0",
  "username": "jsmith",
  "profile": {
    "firstName": "John",
    "lastName": "Smith",
    "email": "john@example.com"
  },
  "interests": ["hiking", "photography", "coding"],
  "posts": [
    {
      "title": "My first blog post",
      "content": "Hello world!",
      "comments": [
        {"user": "alice", "text": "Great post!"}
      ]
    }
  ]
}
```

#### When to Use Document Databases

Document databases excel in scenarios like:

- **Content management systems**: Blogs, articles, product catalogs
- **User profiles**: Variable attributes and preferences
- **E-commerce applications**: Products with different attributes
- **Event logging**: Capturing varied information about events
- **Mobile applications**: Offline-first data synchronization

Document databases are particularly valuable when:
- Your data has a variable structure
- You need to evolve your schema without downtime
- Your data naturally forms hierarchical documents
- You need to query and index nested fields

#### Popular Document Databases

Leading document database technologies include:

- **MongoDB**: The most widely used document database, with robust query capabilities
- **Couchbase**: Combines document model with key-value store performance
- **Amazon DocumentDB**: MongoDB-compatible managed service
- **Azure Cosmos DB**: Multi-model database with document support

In your data engineering career, MongoDB is the most likely document database you'll encounter. According to industry surveys, it's consistently among the top five databases used in modern applications, making it valuable knowledge for your job interviews.



### Key-Value Stores

**Focus:** Understanding key-value database concepts and applications

If you've used HashMaps in Java or objects in JavaScript, you're already familiar with the key-value concept. Key-value stores are essentially distributed, persistent versions of these data structures.

#### Simple Data Model for High-Speed Access

Key-value stores offer a straightforward model:

- Each item is stored as a key-value pair
- Keys are unique identifiers
- Values can be simple strings or complex binary objects
- No query language for the value contents (only key lookups)

This simplicity translates to exceptional performance:

- **Ultra-fast reads and writes** by direct key access
- **Minimal overhead** compared to more complex data models
- **Highly scalable** across distributed systems

#### Use Cases for Key-Value Stores

Key-value stores excel in scenarios requiring:

- **Caching**: Temporary storage of computed results or database queries
- **Session storage**: User session data in web applications
- **User preferences**: Simple configuration settings
- **Real-time analytics**: Counters and time-limited statistics
- **Message queues**: Temporary storage for processing pipelines

For example, an e-commerce site might use a key-value store to track shopping carts:

```
user:12345:cart → {items: [{id: "product1", qty: 2}, {id: "product2", qty: 1}]}
```

This provides sub-millisecond access without complex queries.

#### Popular Key-Value Stores

Leading key-value database technologies include:

- **Redis**: In-memory store with rich data structure support
- **Amazon DynamoDB**: Fully managed, multi-region key-value store
- **Riak**: Distributed key-value store focused on high availability
- **Memcached**: Simple memory-caching system

Redis is particularly valuable to learn, as it's used by over 70% of Fortune 500 companies and appears frequently in data engineering job descriptions. Its versatility extends beyond basic key-value storage to support lists, sets, sorted sets, and even basic stream processing.



### Column-Family Databases

**Focus:** Understanding column-family database concepts and applications

Column-family databases represent a middle ground between the rigid structure of relational databases and the flexibility of document stores.

#### Wide-Column Storage Model

In column-family databases:

- Data is organized into **rows and columns**, similar to tables
- Rows have a unique key
- Each row can have different columns
- Columns are grouped into **column families**
- Data is sparse—rows don't need values for all columns

This structure is particularly efficient for:
- Reading and writing specific columns without loading entire rows
- Storing similar data together for efficient compression
- Handling very wide tables (thousands of columns)

#### Use Cases for Column-Family Databases

Column-family stores excel in scenarios like:

- **Time-series data**: Sensor readings, logs, and metrics
- **IoT applications**: Device telemetry with variable attributes
- **Large-scale analytics**: Historical data with efficient column access
- **Recommendation systems**: Sparse matrices of user preferences
- **Financial data**: Historical price data and transactions

For example, a time-series application might store sensor data like this:

```
Row key: device_id:20230615
Column families:
  - temperature: {09:00: 72.1, 09:15: 72.3, 09:30: 72.8, ...}
  - humidity: {09:00: 40%, 09:15: 41%, 09:30: 42%, ...}
  - pressure: {09:00: 1013hPa, 09:15: 1014hPa, 09:30: 1013hPa, ...}
```

This structure allows efficient queries like "get all temperature readings for device X on date Y" without loading humidity or pressure data.

#### Popular Column-Family Databases

Leading column-family database technologies include:

- **Apache Cassandra**: Highly scalable, designed for high write throughput
- **Apache HBase**: Built on Hadoop, designed for large analytical workloads
- **Google Bigtable**: Google's proprietary column-family database
- **ScyllaDB**: High-performance drop-in replacement for Cassandra

Cassandra knowledge is particularly valuable in data engineering roles, as it's widely used for large-scale data intake scenarios where write performance is critical.



### Graph Databases

**Focus:** Understanding graph database concepts and applications

If you've ever struggled to model relationships in MySQL using multiple joins, you'll appreciate graph databases. They excel at representing and querying connections between entities.

#### Node-Relationship Data Model

Graph databases store data as:

- **Nodes**: Entities with properties (like people, products, locations)
- **Relationships**: Connections between nodes with their own properties
- **Properties**: Attributes of both nodes and relationships

This model directly represents the connections in your data, rather than implying them through foreign keys.

For example, a social network in a relational database requires join tables and complex queries:

```sql
SELECT u1.name, u2.name
FROM users u1
JOIN friendships f ON u1.id = f.user1_id
JOIN users u2 ON f.user2_id = u2.id
WHERE u1.id = 123;
```

In a graph database, the same query is more intuitive:

```
MATCH (user:User {id: 123})-[:FRIENDS_WITH]->(friend:User)
RETURN friend.name
```

#### Use Cases for Graph Databases

Graph databases excel in scenarios with complex relationships:

- **Social networks**: Friend connections, content sharing
- **Recommendation engines**: "Customers who bought X also bought Y"
- **Fraud detection**: Identifying suspicious connection patterns
- **Network and IT operations**: Infrastructure dependencies
- **Knowledge graphs**: Connecting concepts and information

The key advantage is traversing relationships—finding connections like "friends of friends" or "products purchased by customers who also bought this item" becomes simple and performant.

#### Popular Graph Databases

Leading graph database technologies include:

- **Neo4j**: The most widely used graph database
- **Amazon Neptune**: Fully managed graph database service
- **JanusGraph**: Distributed graph database on top of storage backends
- **ArangoDB**: Multi-model database with graph capabilities

While less common than document or key-value stores in entry-level positions, graph database knowledge demonstrates breadth of understanding. Companies with complex relationship data increasingly seek engineers familiar with graph concepts.



## Evaluating NoSQL for Data Engineering Workflows

### ETL Integration Considerations

**Focus:** How NoSQL databases impact ETL design

As a data engineer, you'll often need to extract data from NoSQL sources and transform it for analytical purposes, typically in relational data warehouses. This presents unique challenges compared to relational-to-relational ETL.

#### Extraction Challenges from Schema-less Sources

When extracting from NoSQL databases, you'll face challenges like:

- **Schema discovery**: Understanding what fields exist across documents
- **Field inconsistency**: Handling missing fields or different data types
- **Nested data structures**: Accessing deeply nested fields and arrays
- **Version differences**: Managing schema evolution over time

For example, extracting customer data from MongoDB might require code that handles missing fields and varying structures:

```python
# Schema flexibility requires defensive coding
for customer in mongodb.customers.find():
    customer_name = customer.get('name', 'Unknown')  # Handle missing field
    
    # Handle inconsistent address structure
    if 'address' in customer and isinstance(customer['address'], dict):
        address = customer['address']
        city = address.get('city', 'Unknown')
    else:
        city = 'Unknown'
```

#### Transformation Requirements

Transforming NoSQL data for relational systems involves:

- **Flattening hierarchies**: Converting nested objects to related tables
- **Array handling**: Transforming arrays into related records
- **Type consistency**: Ensuring consistent data types for each column
- **Denormalization decisions**: Choosing what to flatten vs. keep as JSON

For example, transforming a MongoDB document with nested arrays might require multiple output tables:

```
# MongoDB document
{
  "orderId": "12345",
  "customer": "John Smith",
  "items": [
    {"productId": "A1", "quantity": 2, "price": 10.99},
    {"productId": "B2", "quantity": 1, "price": 24.99}
  ]
}

# Transformed to relational tables
# orders table:
# orderId, customer
# 12345, John Smith

# order_items table:
# orderId, productId, quantity, price
# 12345, A1, 2, 10.99
# 12345, B2, 1, 24.99
```

#### Loading Considerations

When loading transformed NoSQL data into relational systems:

- **Incremental loading**: Identifying what has changed since last extraction
- **Data quality**: Validating data meets relational schema requirements
- **Performance optimization**: Bulk loading techniques for large datasets

In interviews, explaining these challenges demonstrates your understanding of real-world ETL complexity. Employers value candidates who recognize the effort involved in bridging different database paradigms.



### Data Architecture Decision Framework

**Focus:** Evaluating when to use NoSQL vs relational databases

As a data engineer, you'll participate in architecture decisions about what database technologies to use. Having a framework for these decisions demonstrates your strategic thinking.

#### Business Requirement Analysis

Start by analyzing requirements across these dimensions:

- **Data structure**: Is the data naturally structured or variable?
- **Scalability needs**: What are the expected data volume and user load?
- **Consistency requirements**: Is immediate consistency critical?
- **Query patterns**: Are queries simple lookups or complex analytics?
- **Development velocity**: How frequently will the schema evolve?

#### Performance, Scalability, and Consistency Trade-offs

Each database type offers different trade-offs:

| Database Type | Consistency | Scalability | Query Flexibility | Schema Flexibility |
|---------------|------------|------------|------------------|-------------------|
| Relational    | High       | Vertical   | High (SQL)       | Low               |
| Document      | Medium     | Horizontal | Medium           | High              |
| Key-Value     | Varies     | Horizontal | Low (key only)   | Highest           |
| Column-Family | Eventually | Horizontal | Medium           | Medium            |
| Graph         | High       | Limited    | High (relationships) | Medium         |

#### Hybrid Architectures

Modern systems often combine multiple database types:

- **Operational data store**: NoSQL for high-velocity transaction processing
- **Analytical data warehouse**: Relational for complex analytics and reporting
- **Caching layer**: Key-value stores for performance optimization
- **Search functionality**: Specialized search engines like Elasticsearch

For example, an e-commerce platform might use:
- MongoDB for product catalog (flexible attributes)
- Redis for shopping carts and sessions (speed)
- PostgreSQL for orders and financial data (ACID compliance)
- Elasticsearch for product search (full-text capabilities)

Being able to explain these hybrid architectures shows interviewers you understand pragmatic solutions rather than dogmatic technology preferences.



### Industry Trends and Career Impact

**Focus:** How NoSQL knowledge impacts data engineering careers

Understanding the NoSQL landscape isn't just theoretical—it directly impacts your career opportunities as a data engineer.

#### Current Industry Adoption

NoSQL adoption continues to grow in specific sectors:

- **Tech companies**: Heavy users of NoSQL for web-scale applications
- **E-commerce**: Document databases for product catalogs, key-value for carts
- **Financial services**: Column-family for time-series data, graph for fraud detection
- **Healthcare**: Document databases for patient records with variable structures
- **IoT/Telemetry**: Column-family databases for sensor data and metrics

According to recent industry surveys, over 70% of organizations use at least one NoSQL database alongside their relational systems.

#### Job Roles Requiring NoSQL Expertise

NoSQL knowledge is valuable across data engineering roles:

- **ETL/Data Integration Engineer**: Extracting from diverse sources including NoSQL
- **Data Platform Engineer**: Building pipelines that accommodate various data types
- **Data Architect**: Making technology selection decisions
- **Analytics Engineer**: Transforming NoSQL data for analytical use

Job postings for entry-level data engineers increasingly mention experience with at least one NoSQL database, most commonly MongoDB and Redis.

#### Skills to Highlight in Interviews

When discussing NoSQL in interviews:

- **Conceptual understanding**: Explain the different types and their use cases
- **Data modeling**: Describe how you'd model the same data in different paradigms
- **Integration knowledge**: Discuss how you'd extract and transform NoSQL data
- **Trade-off awareness**: Show you understand when to use which technology
- **Learning approach**: Demonstrate how you stay current with database technologies

Sharing examples of how you've worked with JSON data in your projects, even if not directly with NoSQL databases, can demonstrate transferable knowledge.



## Hands-On Practice

### Conceptual Exercise: Relational vs. Document Database Design

**Focus:** Comparing data modeling approaches between paradigms

Let's apply what you've learned by examining an e-commerce data model across different database paradigms.

#### Scenario: E-commerce Product Catalog

Consider an e-commerce platform with products that have:
- Basic information (name, price, description)
- Variable attributes (different for each product category)
- Categories and tags
- Inventory levels
- Customer reviews

#### Relational Model

In a relational database, this might be modeled as:

```
products
- product_id (PK)
- name
- price
- description
- category_id (FK)

categories
- category_id (PK)
- name
- parent_category_id (FK)

product_attributes
- attribute_id (PK)
- product_id (FK)
- attribute_name
- attribute_value

product_tags
- product_id (FK)
- tag_id (FK)

tags
- tag_id (PK)
- name

inventory
- product_id (FK)
- warehouse_id (FK)
- quantity

reviews
- review_id (PK)
- product_id (FK)
- customer_id (FK)
- rating
- comment
- review_date
```

Querying for a product with all its information requires multiple joins:

```sql
SELECT p.*, c.name as category_name,
       a.attribute_name, a.attribute_value,
       t.name as tag_name,
       i.quantity,
       r.rating, r.comment
FROM products p
JOIN categories c ON p.category_id = c.category_id
LEFT JOIN product_attributes a ON p.product_id = a.product_id
LEFT JOIN product_tags pt ON p.product_id = pt.product_id
LEFT JOIN tags t ON pt.tag_id = t.tag_id
LEFT JOIN inventory i ON p.product_id = i.product_id
LEFT JOIN reviews r ON p.product_id = r.product_id
WHERE p.product_id = 12345;
```

#### Document Model

In MongoDB, the same data might be modeled as:

```json
{
  "product_id": "12345",
  "name": "Ultra HD Smart TV",
  "price": 799.99,
  "description": "55-inch 4K smart television with...",
  "category": {
    "id": "electronics",
    "name": "Electronics",
    "parent_category": "home"
  },
  "attributes": {
    "screen_size": "55 inches",
    "resolution": "4K",
    "refresh_rate": "120Hz",
    "smart_features": true
  },
  "tags": ["television", "smart tv", "4k", "entertainment"],
  "inventory": [
    {"warehouse": "east", "quantity": 25},
    {"warehouse": "west", "quantity": 13}
  ],
  "reviews": [
    {
      "customer_id": "user123",
      "rating": 4.5,
      "comment": "Great picture quality!",
      "date": "2023-04-15"
    },
    {
      "customer_id": "user456",
      "rating": 5,
      "comment": "Easy setup and excellent value",
      "date": "2023-05-02"
    }
  ]
}
```

Retrieving the complete product information requires a single query:

```javascript
db.products.findOne({ product_id: "12345" })
```

#### Analysis: Pros and Cons

**Relational Model Pros:**
- Enforces consistent structure across all products
- Eliminates data duplication (normalization)
- Allows complex queries across product dimensions
- Supports transactions for inventory updates

**Relational Model Cons:**
- Requires multiple joins for complete product view
- Schema changes need table alterations
- Handling variable attributes requires generic tables
- Performance degrades with many joins

**Document Model Pros:**
- Natural representation of product as a single entity
- Faster retrieval of complete product information
- Easily accommodates variable attributes per product
- Schema can evolve without migrations

**Document Model Cons:**
- Data duplication (category information repeated)
- More complex to query across products (e.g., "find all products with 5-star reviews")
- Potentially inefficient for analytics on specific fields
- Document size limitations for products with many reviews

#### Hybrid Approach Considerations

A real-world solution might use both:
- Document database for product catalog (frontend needs)
- Relational database for inventory, orders, and analytics
- Data replication between systems for different query patterns

This exercise demonstrates how data modeling decisions depend on application requirements, query patterns, and performance needs—a key insight for your data engineering career.



## Conclusion

The NoSQL landscape provides essential tools in a data engineer's toolkit, complementing rather than replacing traditional relational databases. 

We've explored how different consistency models (ACID vs. BASE) influence reliability and scalability, examined the four major NoSQL types with their distinct strengths, and analyzed the practical ETL challenges when working across database paradigms. Understanding these differences allows you to make informed architectural decisions—choosing document stores for flexible schemas, key-value stores for high-speed operations, column-family databases for time-series data, or graph databases for relationship-heavy information. 

This knowledge directly prepares you for our next lesson, where you'll tackle the practical challenges of extracting data from MongoDB collections and transforming it for relational warehouses. Demonstrating this cross-database fluency sets you apart, showing you can design robust pipelines that bridge traditional and modern data architectures.




---



## Glossary

**ACID**: Atomicity, Consistency, Isolation, and Durability - properties of traditional relational databases that ensure reliable transaction processing.

**BASE**: Basically Available, Soft state, Eventually consistent - properties of NoSQL databases that prioritize availability over immediate consistency.

**CAP Theorem**: States that distributed systems can only guarantee two of three properties: Consistency, Availability, and Partition tolerance.

**Column-Family Database**: A NoSQL database type that stores data in columns rather than rows, optimized for reading and writing specific columns efficiently.

**Document Database**: A NoSQL database that stores data in flexible, JSON-like documents without requiring a fixed schema.

**Eventually Consistent**: A consistency model where all database replicas will become consistent over time, but might not be immediately consistent.

**Graph Database**: A NoSQL database optimized for storing and querying relationships between data elements.

**Horizontal Scaling**: Adding more machines to handle increased load (scaling out).

**Key-Value Store**: A simple NoSQL database that stores data as pairs of keys and values, optimized for quick lookups.

**Schema-less**: Database design that doesn't require a predefined structure for stored data.

**Sharding**: The practice of splitting data across multiple machines to handle larger datasets and traffic.

**Vertical Scaling**: Adding more resources (CPU, RAM, etc.) to a single machine (scaling up).




---



## Exercises

None