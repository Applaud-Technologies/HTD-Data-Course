# Azure Data Factory Pipeline & Power BI Dashboard Specifications

## Azure Data Factory Pipeline Specifications

### Pipeline Name
`InsuranceAnalyticsPipeline`

### Activity Specifications

#### Activity 1: Environment Setup
- **Activity Name**: `InsuranceEnvironmentSetup`
- **Activity Type**: Databricks Notebook
- **Notebook Path**: `/path/to/00-Insurance-Environment-Setup`
- **Timeout**: 30 minutes
- **Retry**: 2 attempts
- **Dependencies**: None (first activity)

#### Activity 2: Risk Profiling  
- **Activity Name**: `InsuranceRiskProfiling`
- **Activity Type**: Databricks Notebook
- **Notebook Path**: `/path/to/01-Insurance-Risk-Profiling`
- **Timeout**: 45 minutes
- **Retry**: 2 attempts
- **Dependencies**: Runs after InsuranceEnvironmentSetup succeeds

#### Activity 3: CLPV Retention
- **Activity Name**: `InsuranceCLPVRetention`
- **Activity Type**: Databricks Notebook
- **Notebook Path**: `/path/to/02-Insurance-CLPV-Retention`
- **Timeout**: 60 minutes
- **Retry**: 2 attempts
- **Dependencies**: Runs after InsuranceRiskProfiling succeeds

#### Activity 4: Executive Dashboard
- **Activity Name**: `InsuranceExecutiveDashboard`
- **Activity Type**: Databricks Notebook
- **Notebook Path**: `/path/to/03-Insurance-Executive-Dashboard`
- **Timeout**: 45 minutes
- **Retry**: 2 attempts
- **Dependencies**: Runs after InsuranceCLPVRetention succeeds

#### Activity 5: Success Notification
- **Activity Name**: `SuccessNotification`
- **Activity Type**: Web Activity
- **URL**: Your webhook URL (Teams, Slack, or email service)
- **Method**: POST
- **Dependencies**: Runs after InsuranceExecutiveDashboard succeeds
- **Purpose**: Send success notification when pipeline completes

#### Activity 6: Failure Notification  
- **Activity Name**: `FailureNotification`
- **Activity Type**: Web Activity
- **URL**: Your webhook URL (Teams, Slack, or email service)
- **Method**: POST
- **Dependencies**: Runs if any of the 4 notebook activities fail
- **Purpose**: Send failure alert when pipeline encounters errors

### Pipeline Implementation Instructions

#### Phase 1: Build Core Pipeline (Required)
**Objective**: Create and test the 4-notebook pipeline to ensure it works correctly

1. **Build Initial Pipeline**
   - Create pipeline with only the 4 notebook activities
   - Configure dependencies: Notebook 0 → 1 → 2 → 3
   - Set timeouts and retry settings as specified above
   - Configure daily trigger at 4:00 AM

2. **Test Core Pipeline**
   - Run the pipeline manually to verify all notebooks execute successfully
   - Confirm each notebook completes without errors
   - Verify data flows correctly between notebooks

3. **Phase 1 Deliverable**
   - [ ] **Screenshot**: Pipeline canvas showing 4 notebook activities with dependencies
   - [ ] **Screenshot**: Successful pipeline run showing all 4 notebooks completed

#### Phase 2: Add Web Alerts (Required)
**Objective**: Add monitoring and alerting capabilities using simulated web endpoints

1. **Add Success Notification**
   - Add Web Activity named `SuccessNotification`
   - Configure to run after InsuranceExecutiveDashboard succeeds
   - Use simulated URL: `https://httpbin.org/post` (test endpoint)
   - Method: POST
   - Body: `{"message": "Insurance Analytics Pipeline completed successfully", "status": "success"}`

2. **Add Failure Notification**
   - Add Web Activity named `FailureNotification`
   - Configure to run if any of the 4 notebook activities fail
   - Use simulated URL: `https://httpbin.org/post` (test endpoint)
   - Method: POST
   - Body: `{"message": "Insurance Analytics Pipeline failed", "status": "failure"}`

3. **Test Complete Pipeline**
   - Run the complete 6-activity pipeline
   - Verify web activities execute (they should succeed with the test endpoint)

4. **Phase 2 Deliverable**
   - [ ] **Screenshot**: Complete pipeline canvas showing all 6 activities
   - [ ] **Screenshot**: Web activity configuration showing simulated URL and POST body
   - [ ] **Screenshot**: Successful pipeline run including web activities

### Pipeline Trigger Specifications
- **Trigger Name**: `DailyInsuranceAnalytics`
- **Trigger Type**: Schedule
- **Frequency**: Daily
- **Start Time**: 4:00 AM
- **Time Zone**: Your local time zone

### Final Pipeline Flow (After Both Phases)
```
InsuranceEnvironmentSetup (Notebook 0)
    ↓ (on success)
InsuranceRiskProfiling (Notebook 1)
    ↓ (on success)
InsuranceCLPVRetention (Notebook 2)
    ↓ (on success)
InsuranceExecutiveDashboard (Notebook 3)
    ↓ (on success)
SuccessNotification (Web Activity → https://httpbin.org/post)

FailureNotification (Web Activity → https://httpbin.org/post)
    ↑ (on failure from any notebook activity)
InsuranceEnvironmentSetup, InsuranceRiskProfiling, 
InsuranceCLPVRetention, or InsuranceExecutiveDashboard
```

**Note**: Web activities use simulated endpoints for demonstration purposes.

---

## Power BI Dashboard Specifications

### Data Sources Required
- All tables from Databricks `insurance_analytics` database
- Or exported CSV files from the notebooks

### Required Measures
```
Total Premiums = SUM(policies[premium_amount])
Customer Count = DISTINCTCOUNT(customers[customer_id])
Loss Ratio = DIVIDE(SUM(claims[claim_amount]), SUM(policies[premium_amount]), 0) * 100
Retention Rate = DIVIDE(COUNTROWS(FILTER(policies, policies[policy_status] = "Active")), COUNTROWS(policies), 0) * 100
```

### Page 1: Executive Insurance Overview
**Required Visualizations:**
- **4 KPI Cards**: Total Premiums, Loss Ratio, Customer Count, Retention Rate
- **Line Chart**: Premium trends by month (X: start_date, Y: Total Premiums)
- **Pie Chart**: Policy mix (Legend: policy_type, Values: Total Premiums)
- **Map**: Geographic distribution (Location: state, Size: Total Premiums)

### Page 2: Customer Analytics
**Required Visualizations:**
- **Column Chart**: CLPV distribution (X: customer_lifetime_premium_value, Y: Customer Count)
- **Scatter Chart**: Risk vs Value (X: composite_risk_score, Y: customer_lifetime_premium_value, Legend: risk_category)
- **Table**: Customer segments summary (risk_category, value_segment, Customer Count, Avg CLPV)

### Page 3: Claims and Risk Analysis
**Required Visualizations:**
- **Line Chart**: Claims trends over time (X: claim_date by month, Y: SUM of claim_amount)
- **Table**: High-risk customers (filtered for risk_category = "High")
- **Bar Chart**: Claims by policy type (X: SUM of claim_amount, Y: policy_type)

### Page 4: Business Recommendations
**Required Visualizations:**
- **Matrix**: Customer priority grid (Rows: risk_category, Columns: value_segment, Values: Customer Count)
- **Table**: Action items (top customers by CLPV with risk and value segments)
- **Additional KPI Cards**: Average CLPV, High-Risk Customer Count

### Design Requirements
- **Professional Theme**: Use built-in corporate or executive theme
- **Page Titles**: Clear descriptive titles on each page
- **Consistent Colors**: Apply theme consistently across all visualizations

---

## Submission Requirements

### Azure Data Factory Deliverables

#### Phase 1 Deliverables (Core Pipeline)
- [ ] Screenshot of 4-notebook pipeline canvas showing activities and dependencies
- [ ] Screenshot of successful 4-notebook pipeline execution

#### Phase 2 Deliverables (Complete Pipeline with Alerts)
- [ ] Screenshot of complete 6-activity pipeline canvas (4 notebooks + 2 web activities)
- [ ] Screenshot of web activity configuration showing simulated webhook URL and POST body
- [ ] Screenshot of successful complete pipeline execution including web activities
- [ ] Screenshot of trigger configuration (daily at 4:00 AM)
- [ ] Optional: Export ARM template as `InsuranceAnalyticsPipeline.json`

### Power BI Deliverables  
- [ ] `InsuranceAnalyticsDashboard.pbix` file
- [ ] Screenshots of all 4 dashboard pages
- [ ] Screenshot of data model showing table relationships

### Documentation
- [ ] Brief description of any implementation challenges and solutions