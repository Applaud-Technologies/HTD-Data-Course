# Insurance Analytics Platform - Final Project Assignment



## 🎯 Project Mission

You are a **Data Engineer** for SecureLife Insurance Company tasked with building a comprehensive analytics platform that drives business decisions through data-driven insights. This project will demonstrate your ability to handle real-world data engineering challenges using modern Azure cloud technologies.



## 📅 Project Timeline

- **Project Start**: Friday, July 4th, 2025
- **Final Presentations**: Friday, July 11th, 2025
- **Duration**: 8 days (15-20 hours total effort)
- **Work Pattern**: 2-3 hours daily, heavier on weekends



## 🏗️ Technical Architecture Overview

- **Data Platform**: Azure Databricks with PySpark
- **Pipeline Orchestration**: Azure Data Factory (4-notebook orchestration)
- **Visualization**: Power BI
- **Data Volume**: 15K customers, 75K policies, 12K claims
- **Business Focus**: Pricing optimization, customer retention, risk assessment
- **Professional Architecture**: Progressive complexity from data foundation to business intelligence

## 

## 📋 Dataset Overview

### **Data Location**
All datasets are pre-loaded in Azure Databricks at `/mnt/coursedata/`

### **Core Business Entities**
| Dataset | Records | Primary Key | Business Purpose |
|---------|---------|-------------|------------------|
| **customer_profiles.csv** | 15,000 | customer_id | Demographics, risk profiling, segmentation |
| **policy_details.csv** | 75,000 | policy_id | Coverage analysis, pricing optimization |
| **claims_history.csv** | 12,000 | claim_id | Risk assessment, fraud detection |
| **premium_payments.csv** | 200,000 | payment_id | Cash flow, customer behavior analysis |
| **customer_interactions.csv** | 30,000 | interaction_id | Satisfaction, retention prediction |
| **market_rates.json** | 1 | N/A | Competitive pricing benchmarks |

> **📖 Complete Field Definitions**: See [Insurance Analytics Data Dictionary](week_8d_insurance_data_dictionary.md) (week_8d_insurance_data_dictionary.md) for comprehensive field definitions, data types, business rules, and sample values.



## 📊 Business Context & Goals

### SecureLife Insurance Company Challenges
Your analytics platform must address these critical business needs:

1. **Pricing Optimization**: Develop risk-based pricing strategies to remain competitive while maintaining profitability
2. **Customer Retention**: Predict policy renewals and identify at-risk customers
3. **Risk Assessment**: Analyze claim patterns and detect potential fraud indicators
4. **Cross-Selling**: Identify opportunities to sell additional policies to existing customers
5. **Customer Value**: Calculate Customer Lifetime Premium Value (CLPV) for strategic investment decisions

### Expected Business Impact
- **Revenue Growth**: 5-10% increase through optimized pricing and retention
- **Cost Reduction**: 15-20% reduction in customer acquisition costs
- **Risk Mitigation**: Early identification of high-risk customers and fraud patterns
- **Strategic Planning**: Data-driven portfolio optimization and market expansion



## 🎯 Learning Objectives

By completing this project, you will demonstrate:

✅ **Technical Competencies**
- End-to-end data pipeline development with Azure tools
- Professional 4-notebook architecture with enterprise-grade separation of concerns
- PySpark data processing and analytics implementation
- Business intelligence dashboard creation
- Cloud-based data orchestration and automation

✅ **Business Skills**
- Insurance industry knowledge and KPI calculations
- Translation of technical analysis into business recommendations
- Executive-level communication and presentation
- Project management under time constraints

✅ **Portfolio Development**
- Compelling project for entry-level data engineering interviews
- Hands-on experience with in-demand Azure data stack
- Professional documentation and code quality standards
- Enterprise-grade project architecture



## 🔧 Technical Requirements



### Part 1: Azure Databricks Notebooks (70% of Grade)

#### **4-Notebook Professional Architecture**

**Notebook 0: Environment Setup and Data Validation**
*File: `00-Insurance-Environment-Setup.ipynb`*

- Complete data loading & validation from `/mnt/coursedata/`
- Database table creation and optimization
- Data quality assessment and business rule validation
- Foundation establishment for downstream analytics

**Notebook 1: Customer Risk Profiling**
*File: `01-Insurance-Risk-Profiling.ipynb`*

- Customer risk scoring and segmentation
- Policy portfolio analysis and cross-selling opportunities
- Temporal pattern analysis and seasonal trends

**Notebook 2: CLPV and Retention Modeling**
*File: `02-Insurance-CLPV-Retention.ipynb`*

- Customer Lifetime Premium Value (CLPV) calculation
- Renewal prediction modeling and pricing optimization
- Advanced claims pattern analysis and fraud detection

**Notebook 3: Executive Dashboard and Business Intelligence**
*File: `03-Insurance-Executive-Dashboard.ipynb`*

- Executive insurance KPIs and performance metrics
- Strategic customer portfolio optimization
- Business intelligence export preparation for Power BI



### Part 2: Azure Data Factory Pipeline (20% of Grade)

**Pipeline Configuration**: `InsuranceAnalyticsPipeline`
- Sequential 4-notebook orchestration: Notebook 0 → 1 → 2 → 3
- Daily scheduling at 4:00 AM with retry logic
- Success/failure notifications and monitoring

> **📖 Complete Implementation Guide**: See [ADF Pipeline & Power BI Implementation](week_8c_adf_powerbi_implementation.md) (week_8c_adf_powerbi_implementation.md) for detailed configuration specifications.



### Part 3: Power BI Dashboard (10% of Grade)

**Dashboard Design**: `InsuranceAnalyticsDashboard.pbix`
- **Page 1**: Executive Insurance Overview (KPIs, trends, policy mix, geographic)
- **Page 2**: Customer Analytics (CLPV distribution, risk vs value matrix)
- **Page 3**: Claims and Risk Analysis (trends, loss ratios, high-risk customers)
- **Page 4**: Business Recommendations (action lists, pricing opportunities)



## 📈 Execution Strategy

### **Milestone-Based Approach**
Rather than rigid daily schedules, this project follows **flexible milestone checkpoints** that accommodate personal schedules while ensuring steady progress.

> **📖 Detailed Execution Plan**: See [Project Execution Plan](week_8b_project_plan.md) (week_8b_project_plan.md) for comprehensive milestone timelines, success criteria, and risk management strategies.

### **Recommended Timeline**
- **Weekend (July 4-6)**: Complete Notebooks 0 & 1 (Data foundation & risk analysis)
- **Mid-week (July 7-8)**: Complete Notebook 2 (Advanced analytics)
- **Thursday (July 9-10)**: Complete Notebook 3, Pipeline, and Dashboard
- **Friday (July 11)**: Final testing and presentation delivery



## 🎤 Presentation Requirements

### **Format & Audience**
- **Duration**: 8 minutes presentation + 2 minutes Q&A
- **Audience**: Insurance executives (role-play)
- **Style**: Professional business presentation
- **Goal**: Demonstrate technical skills and business impact

### **Required Content Structure**
1. **Business Problem & Solution** (2 minutes): Industry challenges and your analytical approach
2. **Technical Demonstration** (4 minutes): Live Power BI dashboard and key findings
3. **Business Impact & Recommendations** (2 minutes): Actionable recommendations with ROI



## 📋 Submission Requirements

### **Final Deliverables Package**
- [ ] `00-Insurance-Environment-Setup.ipynb` ⭐ **NEW**
- [ ] `01-Insurance-Risk-Profiling.ipynb`
- [ ] `02-Insurance-CLPV-Retention.ipynb`
- [ ] `03-Insurance-Executive-Dashboard.ipynb`
- [ ] `InsuranceAnalyticsPipeline.json` (ARM template for 4-notebook pipeline)
- [ ] `InsuranceAnalyticsDashboard.pbix`
- [ ] `FinalPresentation.pdf`
- [ ] `PipelineExecution_Screenshots.pdf`
- [ ] `ProjectSummary.pdf` (1-page executive summary)

> **📖 Complete Submission Checklist**: See [Final Deliverables Checklist](week_8e_deliverables_list.md) (week_8e_deliverables_list.md) for detailed requirements and success criteria.



## 🏆 Assessment Rubric (Total: 100 Points)

### **Technical Implementation (70 points)** 
- **Notebook 0 - Environment Setup** (10 points): Data loading, validation, quality assessment
- **Notebook 1 - Risk Profiling** (20 points): Risk assessment, portfolio analysis, pattern recognition
- **Notebook 2 - CLPV & Retention** (25 points): Predictive modeling, pricing optimization, advanced analytics
- **Notebook 3 - Executive Dashboard** (15 points): Business intelligence, KPIs, strategic recommendations

### **Pipeline & Automation (20 points)**
- **4-Notebook ADF Pipeline** (12 points): Configuration, dependencies, scheduling
- **Testing & Documentation** (4 points): Successful runs, screenshots
- **Professional Delivery** (4 points): ARM template, documentation

### **Business Intelligence (10 points)**
- **Dashboard Design** (6 points): Professional appearance, clear visualizations
- **Business Relevance** (4 points): Insurance insights, actionable recommendations

### **Bonus Opportunities (+5 points each, max +10)**
- Advanced analytics beyond requirements
- Exceptional presentation and business insights
- Creative problem-solving approaches
- Outstanding documentation and code quality



## 🚀 Success Strategies

### **Technical Excellence**
- **Foundation First**: Ensure Notebook 0 is rock-solid before proceeding
- **Test Frequently**: Run each notebook incrementally to catch issues early
- **Document Decisions**: Explain business logic and technical choices
- **Validate Results**: Ensure calculations are logical and defensible

### **Business Impact**
- **Think Like an Executive**: Focus on business value and ROI
- **Tell a Story**: Connect technical analysis to business outcomes
- **Specific Recommendations**: Provide actionable insights, not just analysis
- **Industry Context**: Demonstrate understanding of insurance business



## 🎯 Career Development Value

This project positions you for **entry-level data engineering roles** such as:
- Junior Data Engineer
- Business Intelligence Developer
- Insurance Data Analyst
- Azure Data Platform Developer

**Key Skills Demonstrated:**
- Modern Azure data stack proficiency
- Enterprise-grade 4-notebook architecture
- End-to-end project delivery under deadlines
- Business analytics and intelligence
- Professional communication and presentation



---



## 📚 **Supplementary Documentation**

This project includes comprehensive supporting materials:

- **[Project Execution Plan](week_8b_project_plan.md)**: Detailed milestone timelines and risk management
- **[ADF Pipeline & Power BI Implementation](week_8c_adf_powerbi_implementation.md)**: Technical configuration specifications
- **[Insurance Analytics Data Dictionary](week_8d_insurance_data_dictionary.md)**: Complete field definitions and business rules
- **[Final Deliverables Checklist](week_8e_deliverables_list.md)**: Submission requirements and success criteria

**🚀 Project Objective**: Demonstrate enterprise-grade data engineering skills through a complete insurance analytics platform that drives business decisions with data-driven insights.