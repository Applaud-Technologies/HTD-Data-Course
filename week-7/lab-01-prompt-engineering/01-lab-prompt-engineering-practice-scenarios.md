# Lab: Prompt Engineering Practice Scenarios

**Instructions:** For each scenario, create prompts using the appropriate technique (Chain-of-Thought, Few-Shot, or Prompt Chaining) following the patterns shown in the lesson materials. Pay attention to constraint definition, task structure, and output formatting. Test your prompts and refine them based on the AI's responses.

---
## Part 1: Chain-of-Thought Scenario Descriptions

### 1.1 Personal Productivity:

**Scenario: Home Organization System Design**

You need to create a comprehensive home organization system for a working parent with two young children who is overwhelmed by household clutter and wants to establish sustainable organization habits.

**Key Details:**

- Small 3-bedroom house (1,200 sq ft) with limited storage
- Budget constraint of $200 for organizational supplies
- Must accommodate children's toys, work-from-home office supplies, and kitchen items
- Parent has 2 hours on weekends for organization activities
- System must be maintainable by children ages 6 and 9

**Student Task:** Write a chain-of-thought prompt that guides the AI to systematically analyze the space, identify problem areas, evaluate organizational solutions, and create a detailed implementation plan with specific product recommendations and a timeline.

### 1.2 Data Engineer:

**Scenario: Data Pipeline Architecture Design**

A startup needs to design their first data pipeline to process customer behavior data from multiple sources (web analytics, mobile app events, customer service interactions) and load it into a data warehouse for business intelligence reporting.

**Key Details:**

- Processing 10,000 events per hour during peak times
- Must use open-source tools only (budget constraints)
- Team has limited experience with data engineering
- Data sources include JSON APIs, CSV files, and database exports
- Need real-time dashboard updates within 15 minutes of data generation
- Must handle data quality issues and duplicate records

**Student Task:** Create a chain-of-thought prompt that systematically evaluates architecture options, considers scalability requirements, addresses data quality concerns, and produces a detailed technical implementation plan.

### 1.3 Professional Development:

**Scenario: Career Transition Strategy**

A marketing professional with 5 years of experience wants to transition into UX design but has no formal design education or portfolio. They need a comprehensive career transition plan that maximizes their existing skills while building new competencies.

**Key Details:**

- Currently earning $65,000 and cannot take unpaid time off
- Has strong analytical and research skills from marketing background
- Budget of $3,000 for education/certification over 12 months
- Needs to build a portfolio while working full-time
- Target salary of $75,000+ in UX role within 18 months
- Lives in a mid-sized city with moderate tech job market

**Student Task:** Develop a chain-of-thought prompt that systematically analyzes transferable skills, identifies skill gaps, evaluates learning options, and creates a detailed transition timeline with milestones.

## Part 2: Few-Shot Scenario Descriptions

### 2.1 Personal Productivity:

**Scenario: Morning Routine Optimization**

Create a personalized morning routine for busy professionals that maximizes energy and productivity while fitting into different lifestyle constraints.

**Key Details:**

- Must provide examples for different time constraints (30 minutes, 45 minutes, 60 minutes)
- Should accommodate different personality types (morning person vs. night owl)
- Include health-focused activities (exercise, nutrition, mindfulness)
- Consider family responsibilities and commute requirements

**Student Task:** Write a few-shot prompt with 2-3 example morning routines that demonstrate the desired output format and variety, then ask for a routine tailored to specific constraints you define.

### 2.2 Data Engineer:

**Scenario: Database Query Optimization Patterns**

Teach common SQL query optimization techniques through pattern recognition by showing examples of poorly written queries and their optimized versions.

**Key Details:**

- Focus on queries that join 3+ tables
- Address common performance issues (N+1 queries, missing indexes, inefficient WHERE clauses)
- Show before/after examples with explanation of improvements
- Target MySQL or PostgreSQL syntax
- Include execution time improvements

**Student Task:** Create a few-shot prompt with 2-3 examples of query optimization transformations, then provide a new poorly optimized query for the AI to improve following the established patterns.

### 2.3 Professional Development:

**Scenario: LinkedIn Profile Optimization**

Generate compelling LinkedIn profile sections (headlines, summaries, experience descriptions) for different career stages and industries.

**Key Details:**

- Show examples for entry-level, mid-career, and senior-level professionals
- Demonstrate industry-specific language and keywords
- Include metrics and achievements where appropriate
- Balance professional tone with personality
- Optimize for LinkedIn algorithm and recruiter searches

**Student Task:** Develop a few-shot prompt with 2-3 examples of strong LinkedIn profile sections for different roles, then request a profile section for a specific career situation.

## Part 3: Prompt Chaining Scenario Descriptions

### 3.1 Personal Productivity:

**Scenario: Complete Wedding Planning System**

Design a comprehensive wedding planning process that breaks down the overwhelming task into manageable, sequential steps over a 12-month timeline.

**Key Details:**

- Budget range of $25,000-$35,000 for 100 guests
- Couple both works full-time with limited weekend availability
- Must coordinate vendors, guests, legal requirements, and personal preferences
- Include contingency planning for weather/vendor issues
- Balance dream wedding vision with practical constraints

**Student Task:** Create a prompt chain with 4-5 sequential prompts that build upon each other: (1) Establish priorities and budget allocation, (2) Create vendor selection criteria and timeline, (3) Develop detailed monthly task lists, (4) Create coordination and communication systems, (5) Build contingency plans.

### 3.2 Data Engineer:

**Scenario: Complete Data Warehouse Implementation**

Guide the end-to-end implementation of a data warehouse from requirements gathering through deployment and monitoring.

**Key Details:**

- Small retail company with sales, inventory, and customer data
- Moving from Excel-based reporting to proper BI system
- Team of 2 junior data engineers with limited DW experience
- Must integrate with existing POS system and e-commerce platform
- Need automated reporting for executive dashboard
- Budget constraints require cloud-based solution

**Student Task:** Develop a prompt chain with 4-5 sequential prompts: (1) Requirements analysis and data source audit, (2) Schema design and data modeling, (3) ETL pipeline architecture, (4) Implementation roadmap and testing strategy, (5) Deployment and monitoring setup.

### 3.3 Professional Development:

**Scenario: Complete Tech Bootcamp Preparation**

Create a comprehensive preparation plan for someone applying to competitive coding bootcamps who needs to build foundational skills before the application process.

**Key Details:**

- Complete beginner with no programming experience
- Wants to apply to bootcamps in 6 months
- Working full-time in unrelated field (customer service)
- Can dedicate 15-20 hours per week to preparation
- Targeting full-stack web development programs
- Needs to build portfolio projects for application

**Student Task:** Design a prompt chain with 4-5 sequential prompts: (1) Assess current skills and create learning objectives, (2) Design month-by-month curriculum with resources, (3) Plan portfolio projects that demonstrate competency, (4) Prepare application materials and interview strategy, (5) Create study schedule and accountability system.
