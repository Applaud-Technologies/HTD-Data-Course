# Student Guide: Introduction to Data Literacy



## Overview

This lesson introduces **data literacy** as a core skill for entry-level engineers entering data-rich domains such as **insurance, finance, and fintech**. You'll learn to read, write, and communicate data effectively—skills that support everything from product development to regulatory compliance.



## Core Learning Outcomes (Simplified for Reference)

- Apply the principles of **data literacy** in engineering workflows.
- Analyze business metrics and justify decisions with data.
- Evaluate when to use conventional vs. Big Data tools using the **10 Vs**.
- Identify your role at key stages of the **data lifecycle**.
- Communicate across technical and non-technical stakeholders using **shared metrics**.



## Key Vocabulary

| Term | Definition |
|------|------------|
| **Data Literacy** | Ability to read, write, and communicate data in context |
| **Metric-Driven Development (MDD)** | Engineering methodology focused on measurable impact |
| **A/B Testing** | Controlled experiment comparing feature variations using statistical rigor |
| **Instrumentation** | Code-level logging or telemetry that tracks application behavior |
| **Data Contract** | Formal agreement between teams on data structure and definitions |
| **Golden Set Testing** | Using a verified data sample to test transformation pipelines |
| **Data Mesh** | Decentralized approach where each domain owns its analytical data |
| **Chief Data Officer (CDO)** | Executive responsible for data governance and strategy |
| **Data Steward** | Domain expert managing the quality and consistency of specific datasets |
| **Data Democratization** | Making data accessible across roles and departments |
| **Statistical Significance** | Indicates an observed change is likely not due to random chance |



## Core Concepts & Takeaways

### Data Literacy in Practice
- **Reading Data**: Interpreting metrics, dashboards, and visualizations.
- **Writing Data**: Logging structured events, building instrumentation.
- **Communicating Data**: Explaining trends, justifying decisions with evidence.



### Data Fluency vs. Technical Skills

| Technical Skill | Complementary Data Skill |
|-----------------|---------------------------|
| Debugging Code | Debugging faulty metrics |
| Refactoring | Using usage data to prioritize changes |
| Building Features | Measuring feature adoption and impact |

**Analogy**: Code = Engine, Data = GPS → You need both to get anywhere meaningful.



### Metric-Driven Development

**Workflow Comparison**:

| Traditional Dev | Metric-Driven Dev |
|-----------------|-------------------|
| Build → Ship → Hope | Define metrics → Build → Measure → Iterate |

**Infrastructure Required**:
- Logging & telemetry
- Feature flags
- Real-time dashboards
- Access to data (e.g., Power BI, APIs)



### Big Data vs. Conventional Data

**The 10 Vs Framework**:

1. **Volume** – Size (GB vs. TB/PB)
2. **Velocity** – Batch vs. streaming
3. **Variety** – Structured vs. mixed formats
4. **Veracity** – Data confidence
5. **Value** – Business relevance
6. **Validity** – Schema integrity
7. **Variability** – Context sensitivity
8. **Venue** – Centralized vs. distributed
9. **Vocabulary** – Standard terms vs. ontologies
10. **Vagueness** – Clear queries vs. exploration



### The Data Lifecycle

| Stage | Description | Engineer’s Role |
|-------|-------------|-----------------|
| **Generation** | Data created by users, logs, systems | Code instrumentation |
| **Collection** | Data captured & stored | API, logging integration |
| **Processing** | Cleaning, transformation | Data pipelines |
| **Analysis** | Statistical review | Support A/B testing, feature review |
| **Visualization** | Dashboards, reports | Build or use Power BI |
| **Interpretation** | Turning patterns into insights | Explain technical implications |
| **Decision** | Business or feature change | Support with data |
| **Implementation** | Deploy code or changes | Monitor outcomes |
| **Feedback** | Data from changes informs next cycle | Close the loop with new metrics |



## Tools & Technologies Mentioned

| Tool / Concept | Usage Context |
|----------------|----------------|
| **Power BI** (Course Tool) | Self-service dashboards and visualization |
| **Looker / Mode / Grafana** | Other examples of company-wide dashboards |
| **JavaScript & Python** | Code snippets for logging and querying |
| **SQL** | Traditional data querying |
| **MapReduce / Spark** | Big Data processing paradigms |
| **APIs** | Accessing internal metrics programmatically |
| **Feature Flags** | Enable experimentation and metric tracking |
| **Real-Time Dashboards** | Engineer-facing metrics (Power BI substitutes here) |



## Core Diagrams (for reference)

- Build–Measure–Learn Cycle  
- Stakeholder Translation Triangle  
- Engineer’s Data Touchpoints Loop  
- 10 Vs Transition Diagram  
- Data-Driven Org Network Diagram



## Career Alignment

This content directly supports junior engineers in:
- Building audit-ready, observable systems  
- Working with analysts and PMs to define success metrics  
- Understanding how business KPIs influence technical decisions  
- Gaining fluency with dashboard tools like **Power BI**  
- Communicating data-backed decisions in interviews and on the job
