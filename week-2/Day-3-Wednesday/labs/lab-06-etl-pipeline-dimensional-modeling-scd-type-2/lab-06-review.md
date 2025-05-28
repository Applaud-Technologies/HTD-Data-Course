# Lab 06 Review

## Foundational Concepts

### Star Schema Design Questions

- **Q: Why do we need surrogate keys when we already have natural keys like customer_id?**
  - **Short Answer:** Surrogate keys provide stability for SCD Type 2 implementations and improve query performance. Natural keys can change or have gaps, while surrogate keys remain consistent and support historical tracking.
  - **Reference:** See "Implementing Conformed Dimensions Across Systems" in Lesson 1 for surrogate key benefits in dual workloads
  - Focus on: SCD Type 2 requirements, performance benefits, stability over time
  - Reference: Step 2, customer dimension table creation

- **Q: What's the difference between the staging table and the dimension tables? Why not just use the staging data directly?**
  - **Short Answer:** Staging tables hold raw, denormalized data from source systems, while dimension tables provide clean, normalized dimensional structures optimized for analytics. This separation enables data quality control and supports both BI and ML workloads.
  - **Reference:** See "Design Dimensional Models for Dual Workloads" in Lesson 1 for schema separation benefits
  - Focus on: Data normalization, separation of concerns, data quality control
  - Reference: Step 1 vs Step 2 comparison

- **Q: Why is the fact table called "fact_sales" instead of just "sales"? What makes it a "fact"?**
  - **Short Answer:** "Fact" indicates this table stores measurable business events (quantities, amounts) at a specific grain, following dimensional modeling conventions. The naming helps distinguish between operational tables and analytical structures.
  - **Reference:** See "Implement Fact Table Grain Decisions for Dual Workloads" in Lesson 1 for fact table design principles
  - Focus on: Dimensional modeling terminology, measures vs attributes
  - Reference: Step 2, fact table creation

### SCD Type 2 Conceptual Questions

- **Q: When would I use SCD Type 1 vs Type 2? How do I decide?**
  - **Short Answer:** Choose Type 1 for attributes where only current values matter (contact info), Type 2 for attributes requiring historical analysis (customer segments, addresses). Consider storage costs and query complexity in your decision.
  - **Reference:** See "Comparing SCD Strategies for Analysis and Feature Creation" in Lesson 1 for business-driven SCD decisions
  - Focus on: Business requirements, historical tracking needs
  - Reference: Customer dimension (Type 2) vs Product dimension (Type 1)

- **Q: What happens if I need to track history for some columns but not others in the same dimension?**
  - **Short Answer:** Use a hybrid approach within the same dimension - implement Type 2 fields (effective dates) for historical attributes while using Type 1 updates for non-historical ones, or consider Type 3 for limited history.
  - **Reference:** See "Analyzing Dual-Purpose Attribute Hierarchies" in Lesson 1 for mixed SCD strategies
  - Focus on: Mixed SCD strategies, Type 3 alternatives
  - Reference: Lesson 2 material on hybrid approaches

## Technical Implementation Questions

### CTE Pipeline Questions

- **Q: Why use a 3-stage CTE instead of just writing separate queries for each step?**
  - **Short Answer:** The 3-stage pattern provides clear separation of concerns (cleanup, business rules, change detection), improves code maintainability, and enables transaction-safe processing for SCD Type 2 implementations.
  - **Reference:** See "SCD Type 2: Historical Tracking Implementation" in Lesson 2 for multi-step processing patterns
  - Focus on: Code organization, readability, performance, maintainability
  - Reference: Step 3, three-stage CTE structure

- **Q: In the change_detection CTE, why do we use LEFT JOIN instead of INNER JOIN?**
  - **Short Answer:** LEFT JOIN identifies both existing customers (for change detection) and new customers (NULL results), enabling the same CTE to handle both scenarios. INNER JOIN would miss new customers entirely.
  - **Reference:** See "SCD Type 2: Historical Tracking Implementation" in Lesson 2 for change detection logic
  - Focus on: Identifying new customers, handling missing records
  - Reference: Step 3, change detection logic

- **Q: The business_rules stage seems simple - when would this stage be more complex?**
  - **Short Answer:** In production, this stage applies sophisticated business logic like customer segmentation algorithms, data enrichment from external sources, or complex derivations that serve both BI reporting and ML feature engineering needs.
  - **Reference:** See "Applying Beneficial Denormalization Strategies" in Lesson 1 for complex business rule examples
  - Focus on: Real-world business logic, data enrichment scenarios
  - Reference: Step 3, Premium Plus example

### Data Loading Questions

- **Q: Why do we expire records first, then insert new ones? Why not do it in one step?**
  - **Short Answer:** This two-step approach prevents overlapping effective dates, maintains audit trails, and ensures referential integrity. It's safer than attempting complex MERGE operations for SCD Type 2.
  - **Reference:** See "SCD Type 2: Historical Tracking Implementation" in Lesson 2 for proper update sequencing
  - Focus on: Transaction safety, avoiding overlapping dates, audit trail
  - Reference: Step 3, SCD Type 2 update operations

- **Q: How do we ensure the fact table loads get the right version of customer data?**
  - **Short Answer:** Use the is_current flag or effective date ranges to join facts with the appropriate dimension version. This ensures historical accuracy and supports point-in-time reporting requirements.
  - **Reference:** See "Implementing Conformed Dimensions Across Systems" in Lesson 1 for dimension versioning strategies
  - Focus on: is_current flag usage, surrogate key relationships
  - Reference: Step 4, fact table loading

- **Q: The recursive CTE for date dimension looks complex - is there an easier way?**
  - **Short Answer:** While you could use calendar tables or external tools, recursive CTEs give you complete control over date attributes needed for both BI hierarchies and ML time-series features.
  - **Reference:** See "Implementing Conformed Dimensions Across Systems" in Lesson 1 for date dimension design serving dual workloads
  - Focus on: Date dimension population strategies, performance considerations
  - Reference: Step 4, date dimension population

## Error Handling & Troubleshooting

### Common Issues Questions

- **Q: What happens if the staging data has duplicate customer records?**
  - **Short Answer:** Use DISTINCT in the data cleanup stage to eliminate duplicates, but investigate upstream data quality issues. Duplicates can indicate problems with source systems or extraction processes.
  - **Reference:** See "Understanding Dimension Change Scenarios" in Lesson 2 for data quality considerations
  - Focus on: DISTINCT usage, data quality issues, upstream fixes
  - Reference: Step 3, data_cleanup stage

- **Q: My fact table load failed with foreign key errors - how do I debug this?**
  - **Short Answer:** Check dimension loading order, validate surrogate key generation, and ensure referential integrity constraints. Use LEFT JOINs to identify orphaned records before loading facts.
  - **Reference:** See "Justifying Denormalization Decisions Based on Query Patterns" in Lesson 1 for debugging strategies
  - Focus on: Referential integrity, dimension loading order, troubleshooting techniques
  - Reference: Step 4, fact table loading joins

- **Q: The validation queries show failures - what's the best way to fix them?**
  - **Short Answer:** Perform root cause analysis by examining the specific validation failures, then implement data quality fixes at the source or add business rules to handle exceptions appropriately.
  - **Reference:** See "Version-Safe Schema Evolution" in Lesson 2 for validation and error handling strategies
  - Focus on: Root cause analysis, data quality remediation
  - Reference: Step 5, validation framework

### Performance Questions

- **Q: Won't the SCD Type 2 approach make the customer dimension huge over time?**
  - **Short Answer:** Yes, Type 2 dimensions grow with changes. Implement archiving strategies, proper indexing, and consider partitioning for large dimensions. The growth rate depends on change frequency in your business domain.
  - **Reference:** See "Implementing Time-Based Partitioning Benefits" in Lesson 1 for growth management strategies
  - Focus on: Growth patterns, archiving strategies, query performance
  - Reference: SCD Type 2 implications

- **Q: Why use IDENTITY columns for surrogate keys instead of GUIDs?**
  - **Short Answer:** IDENTITY keys are smaller (4 bytes vs 16), provide better query performance, and enable more efficient indexing and joins. GUIDs are only necessary when uniqueness across distributed systems is required.
  - **Reference:** See "Evaluating Star vs Snowflake Schema Trade-offs" in Lesson 1 for performance considerations
  - Focus on: Performance implications, storage efficiency, indexing
  - Reference: Step 2, surrogate key design

## Validation & Data Quality

### Validation Framework Questions

- **Q: Why do we need 4 different types of validation? Isn't referential integrity enough?**
  - **Short Answer:** Comprehensive validation ensures data quality across multiple dimensions: structural integrity (referential), business rules (ranges), temporal logic (dates), and calculation accuracy. Each serves different stakeholder needs.
  - **Reference:** See "Justifying Denormalization Decisions Based on Query Patterns" in Lesson 1 for data quality importance
  - Focus on: Comprehensive data quality, business rule validation
  - Reference: Step 5, validation categories

- **Q: What should I do if a validation check fails in production?**
  - **Short Answer:** Follow established error handling procedures: log the failure, prevent downstream processing, investigate root causes, and implement corrective actions. Have rollback procedures ready for critical failures.
  - **Reference:** See "Version-Safe Schema Evolution" in Lesson 2 for production error handling
  - Focus on: Error handling procedures, rollback strategies, monitoring
  - Reference: Validation framework implementation

- **Q: How often should these validation checks run?**
  - **Short Answer:** Run validation checks after each ETL cycle, with critical checks (referential integrity) running before fact loading. Consider continuous monitoring for high-frequency data updates.
  - **Reference:** See "Safe Dimension Modification Planning" in Lesson 2 for validation scheduling strategies
  - Focus on: ETL scheduling, monitoring strategies, performance impact
  - Reference: Production considerations

## Real-World Application

### Business Context Questions

- **Q: In a real company, who decides which fields should be SCD Type 2?**
  - **Short Answer:** Business stakeholders (analysts, compliance, product managers) define historical tracking requirements based on reporting needs, regulatory requirements, and analytical use cases. Data architects translate these into SCD strategies.
  - **Reference:** See "Analyzing Business Requirements" in Lesson 1 for stakeholder-driven dimensional design
  - Focus on: Business stakeholder involvement, requirements gathering
  - Reference: Business-driven design decisions

- **Q: How would this pattern work with much larger data volumes?**
  - **Short Answer:** Large volumes require partitioning strategies, incremental loading patterns, and specialized indexing. Consider columnar storage and cloud-native approaches for massive scale.
  - **Reference:** See "Implementing Time-Based Partitioning Benefits" in Lesson 1 for scalability strategies
  - Focus on: Scalability considerations, incremental loading, partitioning
  - Reference: Production deployment considerations

- **Q: What if we need to integrate data from multiple source systems?**
  - **Short Answer:** Use conformed dimensions to maintain consistency across source systems, implement master data management for key entities, and design integration patterns that preserve data lineage.
  - **Reference:** See "Implementing Conformed Dimensions Across Systems" in Lesson 1 for multi-source integration
  - Focus on: Data integration patterns, master data management
  - Reference: Multi-source considerations

### Alternative Approaches Questions

- **Q: Could we implement this using stored procedures instead of CTEs?**
  - **Short Answer:** Yes, stored procedures offer modularity and reusability but CTEs provide better readability and transaction safety. Choose based on your organization's maintenance capabilities and performance requirements.
  - **Reference:** See "SCD Type 2: Historical Tracking Implementation" in Lesson 2 for implementation pattern alternatives
  - Focus on: Trade-offs between approaches, maintainability, modularity
  - Reference: Implementation alternatives

- **Q: How would this work in cloud data platforms like Azure Synapse or Snowflake?**
  - **Short Answer:** Cloud platforms offer enhanced features like automatic partitioning, columnar storage, and elastic scaling, but the dimensional modeling principles remain the same. Adapt indexing and partitioning strategies to platform capabilities.
  - **Reference:** See "Design Dimensional Models for Dual Workloads" in Lesson 1 for platform-agnostic principles
  - Focus on: Platform-specific optimizations, modern data stack
  - Reference: Technology evolution

- **Q: What about using MERGE statements instead of separate UPDATE/INSERT?**
  - **Short Answer:** MERGE can work for simple SCD scenarios but becomes complex for Type 2 with effective dating. The separate UPDATE/INSERT approach provides clearer logic and better error handling for SCD Type 2.
  - **Reference:** See "SCD Type 2: Historical Tracking Implementation" in Lesson 2 for MERGE vs. multi-step approaches
  - Focus on: MERGE advantages/disadvantages, complexity trade-offs
  - Reference: SCD implementation alternatives

## Advanced Topics

### Optimization Questions

- **Q: How can I make the CTE pipeline faster for large datasets?**
  - **Short Answer:** Implement proper indexing on join columns, consider batch processing, use incremental loading patterns, and optimize the three-stage CTE sequence with appropriate WHERE clauses.
  - **Reference:** See "Implementing Time-Based Partitioning Benefits" in Lesson 1 for performance optimization
  - Focus on: Indexing strategies, batch processing, incremental loads
  - Reference: Performance optimization

- **Q: Should I create indexes on the effective_date and expiration_date columns?**
  - **Short Answer:** Yes, create composite indexes on (natural_key, effective_date) and (is_current) for Type 2 SCDs. These support both point-in-time queries and current record filtering efficiently.
  - **Reference:** See "Implementing Time-Based Partitioning Benefits" in Lesson 1 for SCD-specific indexing
  - Focus on: SCD-specific indexing strategies, query patterns
  - Reference: Type 2 SCD performance

### Monitoring & Maintenance

- **Q: How do I monitor this ETL pipeline in production?**
  - **Short Answer:** Implement logging at each CTE stage, monitor validation results, track dimension growth rates, and set up alerts for referential integrity failures or unusual change volumes.
  - **Reference:** See "Safe Dimension Modification Planning" in Lesson 2 for production monitoring approaches
  - Focus on: Logging, alerting, performance metrics
  - Reference: Production monitoring needs

- **Q: What happens when dimension changes conflict with each other?**
  - **Short Answer:** Establish clear business rules for conflict resolution, implement "last writer wins" or "business priority" logic, and ensure proper audit trails to track conflicting changes.
  - **Reference:** See "Business Requirements Analysis" in Lesson 2 for conflict resolution strategies
  - Focus on: Conflict resolution, data governance, business rules
  - Reference: Complex change scenarios

## Assessment Preparation

### Understanding Check Questions

- **Q: Can you explain why we need the 3-stage CTE pattern for the assessment?**
  - **Short Answer:** The 3-stage pattern demonstrates your understanding of proper ETL design: data cleanup, business logic application, and change detection. It shows you can implement production-ready SCD Type 2 processing.
  - **Reference:** See "SCD Type 2: Historical Tracking Implementation" in Lesson 2 for the complete pattern explanation
  - Focus on: Assessment requirements, pattern demonstration
  - Reference: Assessment preparation

- **Q: What's the minimum I need to implement to show I understand SCD Type 2?**
  - **Short Answer:** Demonstrate surrogate keys, effective dating, change detection logic, and proper UPDATE/INSERT sequencing. Include validation that proves your historical tracking works correctly.
  - **Reference:** See "SCD Type 2: Historical Tracking Implementation" in Lesson 2 for essential components
  - Focus on: Core concepts, essential components
  - Reference: Success criteria

- **Q: How detailed should my validation framework be?**
  - **Short Answer:** Include the four required categories (referential integrity, range checks, date logic, calculation consistency) with clear pass/fail criteria and proper logging to the validation results table.
  - **Reference:** See "Safe Dimension Modification Planning" in Lesson 2 for validation framework requirements
  - Focus on: Assessment expectations, comprehensive coverage
  - Reference: Validation requirements
