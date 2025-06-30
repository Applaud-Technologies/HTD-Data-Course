# Lab: Multi-Policy Discount Analysis

## Introduction

This comprehensive lab challenges you to build a complete, enterprise-grade discount optimization system using modern Azure cloud technologies. You'll step into the role of a data engineer at US of A Bank, tasked with creating an automated discount analysis pipeline that processes customer data, identifies cross-selling opportunities, and provides actionable insights for maximizing customer lifetime value through strategic discount offerings.

**Business Scenario**: As financial services become increasingly competitive, US of A Bank needs a sophisticated solution that can analyze customer banking relationships alongside insurance policy portfolios to identify optimal discount opportunities. Your organization requires a system that not only calculates appropriate discount eligibility but also provides strategic insights to business stakeholders through comprehensive analytics and automated reporting.

**Technical Challenge**: You'll implement a complete data engineering solution spanning the Azure ecosystem - from data processing and analytics in Databricks to orchestration with Data Factory. This lab demonstrates architecting a production-ready system that showcases enterprise-level data engineering capabilities for financial services optimization.

**Learning Objectives**: This lab tests your ability to integrate multiple Azure services seamlessly, implement sophisticated business logic for financial services, create compelling business intelligence solutions, and follow DevOps best practices. You'll demonstrate proficiency in PySpark, advanced analytics, pipeline orchestration, and professional documentation within a realistic banking context.

**Time Investment**: Plan for 4-6 hours of focused development work. This lab demonstrates modern data engineering skills applied to enterprise-scale financial services optimization and customer relationship management.

The lab is structured in two main parts, each building upon the previous to create a cohesive, end-to-end solution that demonstrates production-ready data engineering skills in a banking environment.

---

## Deliverables

1. **Three Databricks Notebooks** - Complete discount analysis pipeline
2. **Azure Data Factory Pipeline** - Orchestration and automation

---

## Setup

### Provided Files

* ✅ `customer_banking.csv` – 25,000 customer banking profiles and account information
* ✅ `insurance_policies.csv` – 15,000 customer insurance policy portfolios  
* ✅ `discount_rules.json` – business rules for discount eligibility
* 🚧 **Student Implementation** – complete TODO methods in notebook templates

**Note**: This assessment uses enterprise-scale datasets to simulate real-world banking data volumes. Ensure your Spark cluster is appropriately sized and implement proper caching strategies for optimal performance.

---

## Part 1: Azure Databricks Notebooks

**Three Complete Databricks Notebooks implementing comprehensive discount analysis pipeline**

### Notebook 1: `01-Environment-Setup.ipynb` (25 points)

#### Objectives:
- Import required libraries and establish Spark connectivity
- Load and validate all data sources with comprehensive error handling
- Perform thorough data quality analysis and relationship validation
- Create foundational datasets and temporary views for downstream processing

#### Expected Data Files:
- `customer_banking.csv` (25,000 customer banking profiles with 9 columns)
- `insurance_policies.csv` (15,000 insurance policies with 9 columns)
- `discount_rules.json` (4 discount rules in JSON format)

#### Implementation Features:

1. **Environment Initialization**
   - Verify Spark session and display version information
   - Create test DataFrame to validate functionality
   - Display connection status with proper error handling
   - Initialize analysis timestamp and logging

2. **Customer Banking Data Loading**
   - Load CSV with proper schema inference and data type validation
   - Display sample data, schema, and comprehensive statistics
   - Perform null value analysis and data completeness checks
   - Validate customer ID uniqueness and business logic constraints

3. **Insurance Policy Data Loading**
   - Load policy portfolio data with complete validation
   - Generate summary statistics across policy types and coverage amounts
   - Verify policy ID uniqueness and active/inactive status distribution
   - Analyze premium distributions and coverage patterns

4. **Discount Rules Configuration Loading**
   - Parse JSON discount rules with comprehensive error handling
   - Convert to Spark DataFrame for downstream processing
   - Display detailed rule information including eligibility criteria
   - Implement fallback rules if file loading fails

5. **Comprehensive Data Quality Validation**
   - Customer data: Check duplicates, balance ranges, demographic consistency
   - Policy data: Validate premium ranges, coverage amounts, policy type distributions
   - Calculate key business metrics: customer-policy relationships, penetration rates

6. **Data Relationship Validation**
   - Verify referential integrity between customers and policies
   - Identify customers without policies and policies without customer profiles
   - Validate temporal consistency and business rule compliance
   - Generate relationship quality scores and coverage metrics

7. **Temporary View Creation**
   - Create views: `customers`, `policies`, `discount_rules`
   - Create enhanced view: `customer_policy_summary` with joined data
   - Test all views with SQL queries to ensure accessibility
   - Optimize views for downstream processing performance

#### Success Criteria:
- All 3 data files load without errors (25,000 customers, 15,000 policies, 4 rules)
- Zero critical data quality issues detected across enterprise-scale datasets
- 100% referential integrity validation between customers and policies
- Comprehensive data profiling report with actionable insights at scale
- All temporary views created, tested, and optimized for large-scale analysis

---

### Notebook 2: `02-Discount-Analysis.ipynb` (35 points)

#### Objectives:
- Load data and create comprehensive customer policy summaries
- Implement sophisticated discount eligibility business rules
- Perform advanced customer segmentation and behavioral analysis
- Calculate discount gaps and identify optimization opportunities
- Generate prioritized customer action lists for business stakeholders

#### Implementation Features:

1. **Customer Policy Portfolio Analysis**
   - Create comprehensive customer summaries with policy aggregations
   - Calculate policy counts by type (AUTO, HOME, RENTERS)
   - Determine total coverage amounts, premium payments, and current discounts
   - Generate boolean flags for policy combinations and multi-policy indicators

2. **Advanced Discount Eligibility Implementation**
   - **Premium Bundle**: Banking + Home + Auto (15% discount, $5K+ balance, 2+ years)
   - **Urban Bundle**: Banking + Renters + Auto (12% discount, $2.5K+ balance, 1+ years)
   - **Multi-Auto Bundle**: Banking + Multiple Autos (10% discount, $3K+ balance, 1+ years)
   - **Loyalty Bonus**: Long-term customers (5% additional, 10+ years, $10K+ balance)

3. **Discount Gap Analysis**
   - Calculate current vs. eligible discount rates for each customer
   - Identify customers receiving suboptimal discounts
   - Quantify revenue impact of discount optimization
   - Prioritize opportunities by customer value and discount potential

4. **Customer Segmentation Intelligence**
   - Multi-dimensional segmentation by value, policy portfolio, and tenure
   - Behavioral analysis including cross-selling opportunities
   - Risk assessment for customer retention and competitive vulnerability
   - Geographic and demographic analysis for market insights

5. **Business Impact Calculations**
   - Revenue opportunity quantification by customer segment
   - Customer lifetime value impact modeling
   - Competitive risk assessment and retention probability scoring
   - ROI calculations for discount program investments

6. **Action Planning and Prioritization**
   - Customer priority scoring based on multiple business factors
   - Segmented action recommendations by customer tier
   - Implementation timeline suggestions based on business impact
   - Resource allocation guidance for maximum ROI

#### Success Criteria:
- Discount eligibility calculated for all customers with proper business rule implementation
- Customer segmentation provides actionable insights across multiple dimensions  
- Discount gap analysis identifies specific revenue optimization opportunities
- Priority rankings enable focused customer outreach and retention strategies
- Comprehensive action plans ready for business stakeholder implementation

---

### Notebook 3: `03-Revenue-Impact.ipynb` (40 points)

#### Objectives:
- Generate executive-level KPIs and comprehensive business intelligence
- Create sophisticated customer prioritization with composite scoring
- Perform market analysis and competitive positioning assessment
- Calculate detailed ROI models and financial projections
- Provide actionable business recommendations with implementation roadmaps

#### Implementation Features:

1. **Executive Dashboard Creation**
   - Generate comprehensive KPIs for C-level consumption
   - Calculate total revenue impact and investment requirements
   - Create customer portfolio analysis with retention metrics
   - Develop competitive positioning and market opportunity assessments

2. **Advanced Customer Prioritization**
   - Multi-factor scoring system incorporating customer value, urgency, and risk
   - Composite priority rankings with tier-based action recommendations
   - Revenue impact quantification for each priority segment
   - Timeline-based implementation guidance with resource requirements

3. **Market Analysis and Competitive Intelligence**
   - Geographic market analysis with opportunity identification
   - Competitive vulnerability assessment for high-value customers
   - Industry benchmarking context for discount strategy positioning
   - Market penetration analysis with growth opportunity identification

4. **Financial Impact and ROI Modeling**
   - Comprehensive 3-year financial projections with sensitivity analysis
   - Multiple implementation scenarios (conservative, aggressive, phased)
   - Risk assessment with mitigation strategies and contingency planning
   - Break-even analysis and payback period calculations

5. **Business Intelligence Exports**
   - Executive summary datasets optimized for stakeholder consumption
   - Customer action lists with specific recommendations and timelines
   - Market analysis exports for strategic planning purposes
   - Financial projection models for budgeting and resource allocation

6. **Strategic Recommendations and Implementation Planning**
   - Detailed implementation roadmap with phases and milestones
   - Technology and operational requirements for execution
   - Success metrics and KPI framework for ongoing monitoring
   - Change management considerations and stakeholder communication plans

#### Success Criteria:
- Executive-ready dashboard with compelling business case for discount optimization
- Customer prioritization enables targeted, high-impact business actions
- Financial projections provide robust business case with clear ROI expectations
- Market analysis offers strategic insights for competitive positioning
- Implementation roadmap provides clear path from analysis to business value realization

---

## Part 2: Azure Data Factory Pipeline (25 points)

### Multi-Policy Discount Analysis Orchestration Pipeline

#### Pipeline Setup
Create a new pipeline with the following properties:
* **Name**: `MultiPolicyDiscountAnalysis`
* **Description**: `Automated discount analysis and customer optimization pipeline`

#### Databricks Activities

Create three Databricks Notebook activities with the following configurations:

**Activity 1: Environment Setup**
* **Activity Name**: `EnvironmentSetup`
* **Activity Type**: Databricks Notebook
* **Notebook Path**: `/Users/[your-email]/01-Environment-Setup`
* **Timeout**: 45 minutes (`0:45:00`)
* **Retry Count**: 2
* **Retry Interval**: 60 seconds
* **Dependencies**: None (first activity)

**Activity 2: Discount Analysis**
* **Activity Name**: `DiscountAnalysis`
* **Activity Type**: Databricks Notebook  
* **Notebook Path**: `/Users/[your-email]/02-Discount-Analysis`
* **Timeout**: 90 minutes (`1:30:00`)
* **Retry Count**: 2
* **Retry Interval**: 60 seconds
* **Dependencies**: `EnvironmentSetup` (On Success)

**Activity 3: Revenue Impact Analysis**
* **Activity Name**: `RevenueImpactAnalysis`
* **Activity Type**: Databricks Notebook
* **Notebook Path**: `/Users/[your-email]/03-Revenue-Impact`
* **Timeout**: 60 minutes (`1:00:00`)
* **Retry Count**: 2
* **Retry Interval**: 60 seconds
* **Dependencies**: `DiscountAnalysis` (On Success)

#### Pipeline Trigger
Create a trigger with the following properties:
* **Name**: `WeeklyDiscountAnalysisTrigger`
* **Description**: `Weekly execution of discount optimization analysis`
* **Type**: Schedule Trigger
* **Start Date**: Today's date
* **Time Zone**: Your local time zone
* **Recurrence**: Weekly on Monday at 7:00 AM
* **End Date**: One year from start date

#### Deliverables for Part 2
* Screenshot of completed pipeline in ADF designer showing all three activities and dependencies
* Screenshot of successful pipeline test run with execution details
* Screenshot of trigger configuration showing weekly schedule
* Export pipeline as ARM template for deployment (`MultiPolicyDiscountAnalysis.json`)

---

## Technical Implementation Standards

### Data Processing Requirements:
- **Enterprise Scale**: Handle 25,000+ customers and 15,000+ policies efficiently
- **Customer Analysis**: 100% referential integrity between customers and policies at scale
- **Discount Calculations**: Precise business rule implementation with validation across large datasets
- **Segmentation Logic**: Multi-dimensional customer categorization with business relevance at enterprise scale
- **Error Handling**: Comprehensive exception handling with graceful degradation for large-scale processing

### Performance Optimization:
- **Critical**: Strategic use of `.cache()` and `.persist()` for frequently accessed DataFrames with large datasets
- Efficient join operations with broadcast hints for optimal performance at scale
- Optimized aggregations using appropriate Spark functions and partitioning strategies
- Memory-efficient processing designed for enterprise-scale operations (25K+ customers)
- Use of `.coalesce()` or `.repartition()` for optimal data distribution

### Business Logic Validation:
- All discount rules implemented precisely per business requirements
- Customer segmentation produces actionable, meaningful categories
- Financial calculations follow standard business practices
- ROI models use conservative, defensible assumptions

---

## Lab Rubric

### Databricks Notebooks (75 points total)

**Notebook 1 - Environment Setup (25 points)**
- Data loading and validation (10 points)
- Error handling and quality checks (8 points)
- Temporary view creation and testing (7 points)

**Notebook 2 - Discount Analysis (35 points)**
- Business rule implementation (15 points)
- Customer segmentation analysis (10 points)
- Discount gap identification (10 points)

**Notebook 3 - Revenue Impact (40 points)**
- Executive reporting and KPIs (15 points)
- Customer prioritization system (10 points)
- Financial modeling and ROI analysis (10 points)
- Business recommendations (5 points)

### Azure Data Factory Pipeline (25 points)
- Pipeline design and activity configuration (15 points)
- Trigger setup and scheduling (5 points)
- Testing and documentation (5 points)

### Bonus Considerations:
- Code quality and documentation (+5 points)
- Advanced analytics implementation (+5 points)
- Business insight depth and actionability (+5 points)

---

## Success Validation Framework

### Data Quality Assurance:
- All datasets loaded with expected row counts and schemas (25,000 customers, 15,000 policies)
- No null values in critical business logic fields across enterprise-scale datasets
- Referential integrity maintained across all customer-policy relationships at scale
- Business rules applied consistently and accurately across large customer populations

### Business Logic Verification:
- Discount eligibility rules match exact business requirements
- Customer segmentation produces logical, actionable categories
- Financial calculations are accurate and defensible
- Priority rankings make business sense and drive action

### Technical Excellence:
- Code is well-documented and follows best practices for enterprise-scale data processing
- Error handling prevents pipeline failures with large datasets
- Performance is optimized for production deployment with 25K+ customer records
- Pipeline orchestration works reliably and can be scheduled for enterprise operations

### Business Value Creation:
- Analysis provides clear, actionable insights for business stakeholders
- ROI models support investment decision-making
- Customer prioritization enables targeted business actions
- Implementation roadmap provides clear path to value realization

---

## Getting Started

1. **Data Preparation**: Upload the three provided data files to your Databricks workspace
2. **Notebook Development**: Create the three notebooks following the detailed specifications
3. **Pipeline Creation**: Build the Data Factory pipeline with proper activity sequencing
4. **Testing and Validation**: Ensure all components work together seamlessly
5. **Documentation**: Complete all deliverable screenshots and exports

**Note**: Complete solution examples will be published tomorrow for reference and learning.

This lab demonstrates your ability to create enterprise-grade data engineering solutions for financial services, combining technical excellence with business acumen to drive real organizational value.