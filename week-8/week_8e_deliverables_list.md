# Insurance Analytics Platform - Final Deliverables

**Project Duration:** July 4-11, 2025  
**Total Points:** 100 + 10 bonus  
**Due Date:** Friday, July 11th, 2025

---

## 📋 **FINAL DELIVERABLES CHECKLIST**

### **1. Azure Databricks Notebooks (70 points)**

#### **Notebook 0: Environment Setup & Data Validation (10 points)**
- [ ] Load all 6 insurance datasets from `/mnt/coursedata/`
- [ ] Validate data quality and relationships
- [ ] Create database tables and Power BI views
- [ ] **File**: `00-Insurance-Environment-Setup.ipynb`

#### **Notebook 1: Customer Risk Profiling (20 points)**
- [ ] Calculate customer risk scores and segmentation
- [ ] Analyze policy portfolio and cross-selling opportunities
- [ ] Identify seasonal patterns in claims and payments
- [ ] **File**: `01-Insurance-Risk-Profiling.ipynb`

#### **Notebook 2: CLPV & Retention Modeling (25 points)**
- [ ] Calculate Customer Lifetime Premium Value (CLPV)
- [ ] Build renewal prediction models
- [ ] Develop pricing optimization recommendations
- [ ] Implement fraud detection analysis
- [ ] **File**: `02-Insurance-CLPV-Retention.ipynb`

#### **Notebook 3: Executive Dashboard (15 points)**
- [ ] Generate executive KPIs and performance metrics
- [ ] Create strategic customer recommendations
- [ ] Prepare data exports for Power BI
- [ ] **File**: `03-Insurance-Executive-Dashboard.ipynb`

---

### **2. Azure Data Factory Pipeline (20 points)**

#### **Pipeline Configuration**
- [ ] Create `InsuranceAnalyticsPipeline` with 4 notebook activities
- [ ] Configure sequential dependencies: Notebook 0 → 1 → 2 → 3
- [ ] Set up daily scheduling at 4:00 AM
- [ ] Add success/failure web notifications
- [ ] Test complete pipeline execution

#### **Documentation**
- [ ] Screenshots of pipeline configuration
- [ ] Screenshots of successful test runs
- [ ] **File**: `InsuranceAnalyticsPipeline.json` (ARM template)

---

### **3. Power BI Dashboard (10 points)**

#### **Required Pages**
- [ ] **Page 1**: Executive Overview (KPIs, trends, policy mix, geographic map)
- [ ] **Page 2**: Customer Analytics (CLPV distribution, risk vs value matrix)
- [ ] **Page 3**: Claims & Risk Analysis (trends, high-risk customers, loss ratios)
- [ ] **Page 4**: Business Recommendations (customer priority matrix, action items)

#### **Technical Requirements**
- [ ] Connect to Databricks tables or CSV exports
- [ ] Professional design with interactive filters
- [ ] **File**: `InsuranceAnalyticsDashboard.pbix`

---

### **4. Final Presentation (Mandatory)**

#### **Format**
- [ ] 8 minutes presentation + 2 minutes Q&A
- [ ] Business problem, technical demo, recommendations
- [ ] Live Power BI dashboard demonstration
- [ ] **File**: `FinalPresentation.pdf`

---

### **5. Submission Package**

#### **Required Files**
- [ ] `00-Insurance-Environment-Setup.ipynb`
- [ ] `01-Insurance-Risk-Profiling.ipynb`
- [ ] `02-Insurance-CLPV-Retention.ipynb`
- [ ] `03-Insurance-Executive-Dashboard.ipynb`
- [ ] `InsuranceAnalyticsPipeline.json`
- [ ] `InsuranceAnalyticsDashboard.pbix`
- [ ] `FinalPresentation.pdf`
- [ ] `PipelineExecution_Screenshots.pdf`
- [ ] `ProjectSummary.pdf` (1-page executive summary)

---

## 🎯 **Success Criteria**

### **Technical Excellence**
- All 4 notebooks execute successfully in sequence
- Data quality validation passes with realistic results
- CLPV calculations and risk scores are business-logical
- Pipeline orchestrates without errors

### **Business Value**
- Executive KPIs align with insurance industry standards
- Customer segmentation provides actionable insights
- Pricing and retention recommendations include ROI projections
- Dashboard tells coherent business story

### **Professional Delivery**
- Clean, well-documented code with business context
- Professional presentation demonstrating technical competence
- Complete submission package with all required files

---

## 📅 **Recommended Timeline**

- **Weekend (July 4-6)**: Complete Notebooks 0 & 1 (Data foundation & risk analysis)
- **Mid-week (July 7-8)**: Complete Notebook 2 (Advanced analytics)
- **Thursday (July 9-10)**: Complete Notebook 3, Pipeline, and Dashboard
- **Friday (July 11)**: Final testing and presentation delivery

---

**🚀 Project Objective**: Demonstrate enterprise-grade data engineering skills through a complete insurance analytics platform that drives business decisions with data-driven insights.