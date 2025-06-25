# 🚀 Lesson 3A: CI/CD Quick Setup Cheat Sheet

## ⚡ Quick Start (5 Minutes)

### Prerequisites Check
```bash
✅ L01A-C fraud detection notebooks exported as .py files
✅ Lab 01AB-C e-commerce notebooks exported as .py files  
✅ L01C + Lab 01C ARM templates saved locally
✅ Azure subscription ID + Databricks workspace URL ready
```

### Azure DevOps Setup
1. **Go to:** https://dev.azure.com
2. **Create Org:** `[yourname]-data-engineering`
3. **Create Project:** `week-6-data-platforms`
4. **Initialize repo** with main branch

---

## 📁 Repository Structure (Copy/Paste)

```
week-6-data-platforms/
├── .azure-pipelines/
│   ├── fraud-detection-pipeline.yml
│   └── ecommerce-analytics-pipeline.yml
├── fraud-detection/
│   ├── l01a-optimized-processing/     # Your L01A notebooks here
│   ├── l01b-advanced-analytics/       # Your L01B notebooks here
│   └── l01c-integration/              # Your L01C ARM templates here
├── ecommerce-platform/
│   ├── lab-01ab-analytics/            # Your Lab 01AB notebooks here
│   └── lab-01c-production/            # Your Lab 01C ARM templates here
└── README.md
```

---

## 🔑 Authentication Quick Setup

### 1. Get Databricks Token
```bash
Databricks Workspace → Profile → User Settings → Access Tokens
→ Generate New Token → Comment: "Week 6 CI/CD" → 90 days → COPY TOKEN
```

### 2. Create Variable Groups
**Azure DevOps → Pipelines → Library → + Variable Group**

#### Fraud Detection Group: `fraud-detection-dev`
```bash
databricks-host = https://adb-{workspace-id}.{region}.azuredatabricks.net
databricks-token = {your-token} 🔒 LOCK THIS
azure-subscription-id = {your-subscription-id}
resource-group-name = {your-resource-group}
```

#### E-commerce Group: `ecommerce-platform-dev`
```bash
databricks-host = {same-as-above}
databricks-token = {same-as-above} 🔒 LOCK THIS
azure-subscription-id = {same-as-above}
ecommerce-resource-group = {your-resource-group}
```

---

## 📝 Pipeline YAML Templates

### Fraud Detection Pipeline
**File:** `.azure-pipelines/fraud-detection-pipeline.yml`
```yaml
name: 'FraudDetection-$(Date:yyyyMMdd)-$(Rev:r)'
trigger:
  branches: [main]
  paths: [fraud-detection/*]

pool:
  vmImage: 'ubuntu-latest'

variables:
  - group: fraud-detection-dev

stages:
- stage: Deploy
  jobs:
  - job: DeployNotebooks
    steps:
    - script: pip install databricks-cli
    - script: |
        databricks configure --token <<EOF
        $(databricks-host)
        $(databricks-token)
        EOF
    - script: |
        databricks workspace import-dir fraud-detection/l01a-optimized-processing /fraud-detection/l01a
        databricks workspace import-dir fraud-detection/l01b-advanced-analytics /fraud-detection/l01b
```

### E-commerce Pipeline  
**File:** `.azure-pipelines/ecommerce-analytics-pipeline.yml`
```yaml
name: 'Ecommerce-$(Date:yyyyMMdd)-$(Rev:r)'
trigger:
  branches: [main]
  paths: [ecommerce-platform/*]

pool:
  vmImage: 'ubuntu-latest'

variables:
  - group: ecommerce-platform-dev

stages:
- stage: Deploy
  jobs:
  - job: DeployAnalytics
    steps:
    - script: pip install databricks-cli
    - script: |
        databricks configure --token <<EOF
        $(databricks-host)
        $(databricks-token)
        EOF
    - script: |
        databricks workspace import-dir ecommerce-platform/lab-01ab-analytics /ecommerce/analytics
        databricks workspace import-dir ecommerce-platform/lab-01c-production /ecommerce/production
```

---

## ⚡ Quick Commands

### Create Pipeline in Azure DevOps
```bash
Pipelines → New Pipeline → Azure Repos Git → [your-repo] 
→ Existing YAML → Select your .yml file → Save
```

### Test Pipeline
```bash
Go to Pipeline → Run Pipeline → Monitor execution → Check Databricks workspace
```

### Verify Deployment
```bash
Databricks Workspace → Check folders:
✅ /fraud-detection/l01a/ (your L01A notebooks)
✅ /fraud-detection/l01b/ (your L01B notebooks)  
✅ /ecommerce/analytics/ (your Lab 01AB notebooks)
✅ /ecommerce/production/ (your Lab 01C notebooks)
```

---

## 🔧 Troubleshooting Quick Fixes

### Authentication Fails
```bash
❌ Error 401 → Check token hasn't expired
❌ "Invalid token" → Regenerate Databricks token
❌ "Variable not found" → Check variable group name + 🔒 lock
```

### Pipeline Fails
```bash
❌ YAML syntax → Use Azure DevOps validator
❌ "Path not found" → Check file paths in repo match YAML
❌ "Permission denied" → Check Databricks workspace access
```

### Deployment Issues
```bash
❌ Notebooks not appearing → Check folder permissions in Databricks
❌ "Import failed" → Ensure files are .py format (not .ipynb)
❌ Timeout → Increase timeout in pipeline settings
```

---

## ✅ Success Validation (2 Minutes)

### Quick Test
1. **Make change** to any notebook in repo
2. **Commit to main** branch  
3. **Watch pipeline** auto-trigger
4. **Check Databricks** - change appears in workspace
5. **Total time** < 5 minutes

### Ready for Lesson 3B When:
```bash
✅ Both platforms deploy automatically
✅ Changes trigger deployments  
✅ Notebooks appear in Databricks correctly
✅ Deployment time < 10 minutes total
✅ You can explain the flow: Git → Azure DevOps → Databricks
```

---

## 🎯 Key URLs & Paths

| Resource | URL/Path |
|----------|----------|
| **Azure DevOps** | https://dev.azure.com |
| **Your Organization** | https://dev.azure.com/[yourname]-data-engineering |
| **Your Project** | https://dev.azure.com/[yourname]-data-engineering/week-6-data-platforms |
| **Pipelines** | Your Project → Pipelines → Pipelines |
| **Variable Groups** | Your Project → Pipelines → Library |
| **Repository** | Your Project → Repos → Files |

---

## 📊 Time Estimates

| Task | Time | Notes |
|------|------|-------|
| Azure DevOps Setup | 5 min | Organization + project creation |
| Repository Structure | 10 min | Upload your completed work |
| Authentication Setup | 10 min | Tokens + variable groups |
| Pipeline Creation | 10 min | YAML files + pipeline setup |
| Testing & Validation | 5 min | Run pipelines + verify |
| **Total** | **40 min** | **Complete automation setup** |

---

## 🚀 Next Steps

**After Lesson 3A Success:**
- **Lesson 3B:** Upgrade to service principals (enterprise auth)
- **Lesson 3C:** Add testing, monitoring, production practices
- **Portfolio:** Document your automated platforms for interviews

**Business Value Achieved:**
- ⚡ **Speed:** 2+ hours manual → 5 minutes automated
- ✅ **Reliability:** No more manual deployment errors  
- 🔄 **Repeatability:** Deploy fearlessly and frequently
- 👥 **Team Ready:** Foundation for enterprise collaboration