# Prompting Examples

A supplement for lesson "Introduction to Prompt Engineering for Productivity."

## Part 1: Chain-of-Thought Prompt Examples

### 1.1 Personal Productivity:

```
You are an expert productivity coach specializing in time management and workplace efficiency, with extensive experience helping professionals streamline communication workflows, particularly for those managing high email volumes.

**Constraints:**
  - Time Limit: Maximum 30 minutes daily for all email management activities
  - Corporate Environment: Must work within standard Gmail/Outlook corporate settings with no third-party apps allowed
  - Response Standards: Must maintain professional 24-hour response time for internal communications

**Task**: 
  1. List strategies to manage email overload for an entry-level data engineer receiving 50 daily emails.
  2. Evaluate the effort required for each strategy, considering time and complexity.
  3. Recommend a prioritized plan based on effectiveness and low effort, tailored for an entry-level data engineer.
  4. Create a detailed daily plan for handling 50 emails, incorporating the recommended strategies.

**Output Format:**
**STRATEGIES & EVALUATION**
Present as a table with columns: Strategy Name | Implementation Effort (Low/Medium/High) | Time Required | Expected Impact (1-5)

**PRIORITIZED PLAN**  
List top 3 recommended strategies in priority order with brief rationale for each

**DAILY EMAIL SCHEDULE**
Present as a time-blocked schedule:
- Morning (X minutes): [specific actions]
- Midday (X minutes): [specific actions]  
- End of day (X minutes): [specific actions]
Total: 30 minutes maximum
```


### 1.2 Data Engineer:

```
You are a senior data engineer with extensive expertise in database performance tuning and SQL query optimization.

**Constraints**:
  - Database System: Solutions must work on MySQL 8.0 with standard configuration (no advanced enterprise features)
  - Index Limitation: Can create maximum 2 new indexes due to storage and maintenance overhead restrictions
  - Execution Target: Optimized query must run under 15 seconds on tables with 500K+ rows each

**Task**: 
  1. Identify common bottlenecks in a slow SQL query for an entry-level data engineer working on large datasets.
  2. Suggest specific improvements to address these bottlenecks, focusing on techniques accessible to beginners.
  3. Rewrite the following SQL query to optimize its performance, ensuring clarity and efficiency: [query].
     
<query>
-- Sample query analyzing customer purchase patterns for Q1 2024
-- This query has multiple performance bottlenecks

SELECT *,
       (SELECT COUNT(*) 
        FROM orders o2 
        WHERE o2.customer_id = c.customer_id 
        AND YEAR(o2.order_date) = 2024) as total_orders_2024,
       (SELECT AVG(oi.quantity * p.price) 
        FROM order_items oi 
        JOIN products p ON oi.product_id = p.product_id 
        WHERE oi.order_id IN (SELECT order_id 
                              FROM orders o3 
                              WHERE o3.customer_id = c.customer_id)) as avg_order_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id  
JOIN products p ON oi.product_id = p.product_id
JOIN categories cat ON p.category_id = cat.category_id
WHERE UPPER(c.status) = 'ACTIVE'
  AND MONTH(o.order_date) IN (1, 2, 3)
  AND YEAR(o.order_date) = 2024
  AND cat.category_name IN ('Electronics', 'Books', 'Clothing')
ORDER BY c.customer_id, o.order_date;
</query>
```

> **NOTE**: XML tags are effective patterns to encapsulate information within a prompt to encourage the LLM to evaluate it as a distinct object and for human readability.

> **NOTE**: We can use parameterization by defining a parameter within the text of a prompt template and assigning its value later. The XML tag comes in handy for assigning the value of the parameter.

This query represents a realistic scenario where a data engineer is trying to analyze customer purchase patterns for Q1 2024. It contains several common performance bottlenecks that junior data engineers often encounter when transitioning from smaller datasets to enterprise-scale data:

**Key Bottlenecks Present:**
- `SELECT *`** instead of specific columns
- Correlated subqueries** in the SELECT clause that execute for every row
- Functions in WHERE clauses** (UPPER, MONTH, YEAR) that prevent index usage
- Unnecessary JOINs** creating large intermediate result sets
- Complex nested subqueries** for calculations
- No result limiting** potentially returning massive datasets

This query would perform poorly on tables with millions of rows. The business logic is sound (analyzing customer purchase behavior), but the implementation demonstrates the gap between functional SQL knowledge and performance-optimized queries.

### 1.3 Professional Development:

```
You are an experienced career mentor specializing in professional development, with a focus on helping entry-level engineers to enhance their workplace skills, particularly in time management.

**Constraints:**
  - Time Budget: Maximum 20 minutes daily for time management practice and skill development
  - Work Environment: Remote work setting with frequent video calls and asynchronous collaboration
  - Goal Timeline: Must show measurable improvement in productivity within 4 weeks

**Task**:
To learn time management, list techniques, explain benefits, then suggest a practice schedule. Now create a schedule for me.
```


## Part 2: Few-Shot with Template Examples

### 2.1 Personal Productivity:

```
You are a fitness coach skilled in creating concise and effective workout plans for busy professionals.

**Constraints:**
- Equipment: No equipment available - bodyweight exercises only
- Space: Must work in a small living room or bedroom (6x6 feet maximum)
- Noise Level: Low-impact exercises only (no jumping or loud movements to avoid disturbing neighbors/family)

**Task**: Review the example workout plans.

<Example_1>
  <Task>
  Plan a workout.
  </Task>
  <Output>
  **Home Work Plan**
  -	Warm-up (5 min: jumping jacks, arm circles)
  -	Strength (10 min: push-ups, squats)
  -	Cardio (10 min: high knees, burpees)
  -	Cool-down (5 min: stretching)
  </Output>
</Example_1>

<Example_2>
  <Task>
  Plan a workout.
  </Task>
  <Output>
  **Home Work Plan**
  -	Warm-up (5 min: dynamic stretches)
  -	Strength (12 min: lunges, plank)
  -	Cardio (8 min: mountain climbers)
  -	Cool-down (5 min: yoga stretches)
  </Output>
</Example_2>

Now plan a 30-minute home workout.
```

> **NOTE**: A set of nested XML tags are used in this example to define the organization of the example and to allow easy editing of the examples to fit new scenarios.

> **NOTE**: The output in this example is inferred by the provided examples.

### 2.2 Data Engineer:

```
You are a senior data engineer with expertise in designing robust data validation rules to ensure data integrity and quality in large-scale datasets.

**Constraints:**
  - Performance: Validation rules must execute in under 100ms per 1000 records on Apache Spark clusters
  - Technology: Must be compatible with Spark SQL validation framework (no complex regex that causes performance issues)
  - Business Rule: Cannot reject more than 5% of incoming records to prevent significant data loss

**Task**: Review the following validation rules.

<Example_1>
Task: Write a data validation rule. 
Output: Ensure the 'email' column contains valid email addresses by checking for the presence of '@' and a valid domain (e.g., '.com', '.org') using a regex pattern: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`.
</Example_1>

<Example_2>
Task: Write a data validation rule. 
Output: Verify the 'age' column contains only positive integers between 18 and 120, rejecting null values or non-numeric entries.
</Example_2>

Now write a rule for this column: [description].

<description>
The 'order_date' column contains valid dates in the format 'YYYY-MM-DD' between January 1, 2000, and the current date. It should reject null values or invalid date formats.
</description>
```


> **NOTE**: The output in this example is inferred by the provided examples.

### 2.3 Professional Development:

```
You are a career coach specializing in professional development, with expertise in crafting strategic networking plans to help professionals build meaningful connections in their industries.

**Constraints:**
  - Time Budget: Maximum 3-4 hours per week for networking activities while maintaining current job responsibilities
  - Budget Limitation: Free networking opportunities only (no paid conferences, premium memberships, or expensive events)
  - Geographic Scope: Must include both local (within 25 miles) and remote/virtual networking options

**Task**: Review the two networking examples.

<Example_1>
Task: Create a networking plan. 
Output: Identify 3 industry events in the next 3 months (e.g., conferences, webinars), connect with 5 professionals on LinkedIn weekly with personalized messages, and schedule 2 informational interviews monthly to discuss career insights.
</Example_1>

<Example_2>
Task: Create a networking plan. 
Output: Join 2 relevant professional groups (e.g., local meetups, online forums), attend 1 virtual or in-person networking event monthly, and follow up with 3 new contacts via email within 48 hours of meeting to maintain relationships.
</Example_2>

Now create a networking plan for a new job.
```

**Pro-Tip:** Instruct the LLM to ask clarifying questions to tailor and refine a template to your circumstance or scenario. Append the following sentence to the prompt template above.

```
Ask any clarifying questions you have to help you provide the best response.
```



## Part 3: Prompt Chaining Examples

### 3.1 Personal Productivity:

```
You are my productivity coach.

During the next 90 days, I want to learn to cook six healthy, easy-to-prepare meals based on beef, chicken, and pork as the protein sources so that I can lose weight, save money, and entertain friends.

**Constraints:**
  - Time Budget: Maximum 2 hours per week for meal prep and cooking (including shopping time)
  - Budget Limit: $75 per week grocery budget for all meals and ingredients
  - Kitchen Equipment: Basic kitchen setup only (stovetop, oven, basic pots/pans - no specialty appliances or gadgets)

Please execute the following prompts in the order they appear. Use the result of each as the input for the next.
1. List 5 goals for the next quarter.
2. Break each goal into 3 actionable steps.
3. Create a weekly checklist based on these steps.
```


### 3.2 Data Engineer:

```
You are a Data Engineer with expertise in SQL queries.

**Constraints:**
  - Database System: Must use MySQL 8.0 syntax only (no advanced PostgreSQL or other database-specific features)
  - Performance Requirement: All queries must execute in under 5 seconds on tables with up to 100K records each
  - Access Level: Read-only permissions (no CREATE, INSERT, UPDATE, or DELETE operations allowed)

1. Summarize the schema of this database.
2. Write a SQL query to join the `customer` and `order` tables from the schema.
3. Explain the query results in plain English.

Database Schema:

# E-commerce Database Schema

## Table 1: CUSTOMERS

Stores customer information and contact details.

|Column|Type|Constraints|Description|
|---|---|---|---|
|customer_id|INT|PRIMARY KEY|Unique customer identifier|
|first_name|VARCHAR(50)|NOT NULL|Customer's first name|
|last_name|VARCHAR(50)|NOT NULL|Customer's last name|
|email|VARCHAR(100)|UNIQUE, NOT NULL|Customer's email address|
|phone|VARCHAR(15)|NULL|Customer's phone number|
|registration_date|DATE|NOT NULL|Date customer registered|
|city|VARCHAR(50)|NULL|Customer's city|
|state|VARCHAR(50)|NULL|Customer's state|
|country|VARCHAR(50)|DEFAULT 'USA'|Customer's country|

## Table 2: CATEGORIES

Product categories for organization.

|Column|Type|Constraints|Description|
|---|---|---|---|
|category_id|INT|PRIMARY KEY|Unique category identifier|
|category_name|VARCHAR(50)|NOT NULL|Category name|
|description|TEXT|NULL|Category description|
|created_date|DATE|NOT NULL|Date category was created|

## Table 3: PRODUCTS

Product catalog with category relationships.

|Column|Type|Constraints|Description|
|---|---|---|---|
|product_id|INT|PRIMARY KEY|Unique product identifier|
|product_name|VARCHAR(100)|NOT NULL|Product name|
|category_id|INT|NOT NULL, FK → categories.category_id|Product category|
|price|DECIMAL(10,2)|NOT NULL|Product price|
|stock_quantity|INT|NOT NULL, DEFAULT 0|Current inventory level|
|description|TEXT|NULL|Product description|
|created_date|DATE|NOT NULL|Date product was added|
|is_active|BOOLEAN|DEFAULT TRUE|Whether product is active|

## Table 4: ORDERS

Customer order information.

|Column|Type|Constraints|Description|
|---|---|---|---|
|order_id|INT|PRIMARY KEY|Unique order identifier|
|customer_id|INT|NOT NULL, FK → customers.customer_id|Customer who placed order|
|order_date|DATETIME|NOT NULL|When order was placed|
|total_amount|DECIMAL(10,2)|NOT NULL|Total order amount|
|order_status|VARCHAR(20)|DEFAULT 'Pending'|Current order status|
|shipping_address|TEXT|NULL|Where to ship order|
|payment_method|VARCHAR(20)|NULL|How customer paid|

## Table 5: ORDER_ITEMS

Junction table linking orders and products (many-to-many relationship).

|Column|Type|Constraints|Description|
|---|---|---|---|
|order_item_id|INT|PRIMARY KEY|Unique order item identifier|
|order_id|INT|NOT NULL, FK → orders.order_id|Order this item belongs to|
|product_id|INT|NOT NULL, FK → products.product_id|Product being ordered|
|quantity|INT|NOT NULL|Quantity of product ordered|
|unit_price|DECIMAL(10,2)|NOT NULL|Price per unit at time of order|
|total_price|DECIMAL(10,2)|NOT NULL|Total price for this line item|

## Table 6: SUPPLIERS

Product suppliers information.

|Column|Type|Constraints|Description|
|---|---|---|---|
|supplier_id|INT|PRIMARY KEY|Unique supplier identifier|
|supplier_name|VARCHAR(100)|NOT NULL|Supplier company name|
|contact_email|VARCHAR(100)|NULL|Supplier contact email|
|phone|VARCHAR(15)|NULL|Supplier phone number|
|address|TEXT|NULL|Supplier street address|
|city|VARCHAR(50)|NULL|Supplier city|
|country|VARCHAR(50)|NULL|Supplier country|

## Table 7: PRODUCT_SUPPLIERS

Junction table for products and suppliers (many-to-many relationship).

|Column|Type|Constraints|Description|
|---|---|---|---|
|product_id|INT|PRIMARY KEY (Composite), FK → products.product_id|Product being supplied|
|supplier_id|INT|PRIMARY KEY (Composite), FK → suppliers.supplier_id|Supplier providing product|
|supply_price|DECIMAL(10,2)|NULL|Price supplier charges|
|is_primary_supplier|BOOLEAN|DEFAULT FALSE|Whether this is the primary supplier|
```


### 3.3 Professional Development:

```
You are an experienced career coach specializing in data engineering, with deep knowledge of professional development pathways for entry-level data engineers who have completed full-stack software development and data engineering boot camps.

**Constraints:**
  - Time Frame: Must be achievable within 6 months while working full-time in current role
  - Study Schedule: Maximum 10 hours per week available for certification preparation
  - Career Focus: Must align with cloud-based data engineering roles (prioritize AWS, Azure, or GCP platforms)

**Task**: 
  1. Identify 3 certifications relevant to data engineering for new boot camp graduates starting as entry-level data engineers.
  2. Compare their costs, durations, and core skills taught, focusing on applicability to entry-level roles.
  3. Recommend one certification that fits a $500 budget and best supports career growth in data engineering.

**Output**:
**CERTIFICATION COMPARISON TABLE**
| Certification | Cost | Duration | Core Skills | Entry-Level Fit (1-5) | Cloud Platform |

**RECOMMENDED CERTIFICATION**
**Choice:** [Certification Name]
**Rationale:** [2-3 sentence explanation covering budget fit, career growth potential, and constraint alignment]
**Next Steps:** [3 bullet points for getting started]
```