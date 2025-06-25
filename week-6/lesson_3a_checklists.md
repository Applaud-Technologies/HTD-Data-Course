# Lesson 3A: DevOps Foundations & Quick Win - Student Checklists

## 📋 Pre-Lesson Preparation Checklist

**Complete this before starting Lesson 3A:**

### Your Completed Work Verification
- [ ] **L01A PySpark Optimization**: Can you access your completed broadcast join and caching notebooks?
- [ ] **L01B SparkSQL Analytics**: Can you access your completed window functions and customer behavior notebooks?
- [ ] **L01C ADF Integration**: Can you access your exported ADF pipeline ARM templates?
- [ ] **Lab 01AB E-commerce Analytics**: Can you access your customer behavior and inventory optimization notebooks?
- [ ] **Lab 01C Production ADF**: Can you access your production e-commerce ADF pipeline ARM templates?

### Azure Environment Check
- [ ] **Azure Account**: You have active Azure trial account with valid subscription
- [ ] **Databricks Workspace**: Your Databricks workspace is running and accessible
- [ ] **Resource Group**: You know your resource group name where Databricks lives
- [ ] **Subscription ID**: You have your Azure subscription ID ready
- [ ] **Admin Access**: You have owner or contributor permissions on your resource group

### File Organization
- [ ] **Notebook Exports**: Your L01A-C notebooks are exported as .py files
- [ ] **Lab Notebook Exports**: Your Lab 01AB notebooks are exported as .py files  
- [ ] **ARM Templates**: Your L01C and Lab 01C ARM templates are saved locally
- [ ] **File Names**: All files have clear, descriptive names
- [ ] **File Structure**: Files are organized by platform (fraud-detection vs ecommerce)

**✅ Pre-Lesson Complete**: All checkboxes marked = Ready to start Lesson 3A

---

## 📋 Azure DevOps Setup Checklist

**Follow this checklist during Azure DevOps setup (20 minutes):**

### Step 1: Organization Creation
- [ ] **Navigate**: Go to https://dev.azure.com in your browser
- [ ] **Sign In**: Use your Azure trial account credentials
- [ ] **Create Organization**: Click "Create organization"
- [ ] **Organization Name**: Use format `[yourname]-data-engineering` (e.g., `sarah-data-engineering`)
- [ ] **Location**: Select your region (same as your Azure region if possible)
- [ ] **Verify**: Organization created successfully and you can access it

### Step 2: Project Creation  
- [ ] **Create Project**: Click "Create project" 
- [ ] **Project Name**: `week-6-data-platforms`
- [ ] **Description**: `CI/CD for L01A-C fraud detection and Lab 01AB-C e-commerce platforms`
- [ ] **Visibility**: Private
- [ ] **Version Control**: Git
- [ ] **Work Item Process**: Basic
- [ ] **Verify**: Project created and you can access Repos section

### Step 3: Repository Initialization
- [ ] **Initialize Repo**: Click "Initialize" to create main branch
- [ ] **Clone URL**: Copy the clone URL for reference
- [ ] **Default Branch**: Confirm main branch is created
- [ ] **Access Test**: Can you see the empty repository structure?

**✅ Azure DevOps Setup Complete**: Organization and project ready for your platforms

---

## 📋 Repository Structure Setup Checklist

**Follow this checklist to organize your completed work (15 minutes):**

### Fraud Detection Platform Structure
- [ ] **Create Folder**: `fraud-detection/`
- [ ] **Create Subfolder**: `fraud-detection/l01a-optimized-processing/`
- [ ] **Upload L01A Work**: Upload your broadcast join and caching notebooks to l01a-optimized-processing/
- [ ] **Create Subfolder**: `fraud-detection/l01b-advanced-analytics/`
- [ ] **Upload L01B Work**: Upload your window functions and analytics notebooks to l01b-advanced-analytics/
- [ ] **Create Subfolder**: `fraud-detection/l01c-integration/`
- [ ] **Upload L01C Work**: Upload your ADF pipeline JSON/ARM templates to l01c-integration/

### E-commerce Platform Structure
- [ ] **Create Folder**: `ecommerce-platform/`
- [ ] **Create Subfolder**: `ecommerce-platform/lab-01ab-analytics/`
- [ ] **Upload Lab 01AB Work**: Upload your customer analytics and inventory optimization notebooks to lab-01ab-analytics/
- [ ] **Create Subfolder**: `ecommerce-platform/lab-01c-production/`
- [ ] **Upload Lab 01C Work**: Upload your production ADF pipeline files to lab-01c-production/

### Pipeline Structure
- [ ] **Create Folder**: `.azure-pipelines/`
- [ ] **Create Subfolder**: `templates/`
- [ ] **Create Folder**: `tests/`
- [ ] **Create Folder**: `infrastructure/`
- [ ] **Create Subfolder**: `infrastructure/arm-templates/`

### Documentation
- [ ] **Create File**: `README.md` with basic project description
- [ ] **Document Platforms**: Add description of your fraud detection platform (L01A-C)
- [ ] **Document Platforms**: Add description of your e-commerce platform (Lab 01AB-C)
- [ ] **Commit Changes**: Commit all files with message "Initial platform setup with completed work"

**✅ Repository Structure Complete**: Your completed work is organized and committed

---

## 📋 Authentication Setup Checklist

**Follow this checklist for Databricks authentication (15 minutes):**

### Step 1: Generate Databricks Personal Access Token
- [ ] **Open Databricks**: Navigate to your Databricks workspace
- [ ] **User Settings**: Click your profile (top right) → "User Settings"
- [ ] **Access Tokens**: Click "Access tokens" tab
- [ ] **Generate Token**: Click "Generate new token"
- [ ] **Token Comment**: Enter "Week 6 Platforms CI/CD"
- [ ] **Token Lifetime**: Set to 90 days
- [ ] **Generate**: Click "Generate"
- [ ] **Copy Token**: **IMMEDIATELY copy the token** (you won't see it again!)
- [ ] **Save Securely**: Paste token in secure location (notepad, password manager)

### Step 2: Get Databricks Workspace URL
- [ ] **Copy URL**: Copy your Databricks workspace URL from browser
- [ ] **Verify Format**: Should be `https://adb-{workspace-id}.{region}.azuredatabricks.net`
- [ ] **Test URL**: Ensure URL opens your workspace in new browser tab
- [ ] **Save URL**: Save workspace URL securely with your token

### Step 3: Create Variable Groups for Fraud Detection Platform
- [ ] **Navigate**: Go to Azure DevOps → Pipelines → Library
- [ ] **Create Group**: Click "+ Variable group"
- [ ] **Group Name**: `fraud-detection-dev`
- [ ] **Description**: `Development environment for L01A+L01B+L01C fraud platform`
- [ ] **Add Variable**: `databricks-host` = `your-databricks-workspace-url`
- [ ] **Add Variable**: `databricks-token` = `your-copied-token`
- [ ] **Secure Token**: Click lock icon 🔒 next to databricks-token
- [ ] **Add Variable**: `azure-subscription-id` = `your-subscription-id`
- [ ] **Add Variable**: `resource-group-name` = `your-resource-group`
- [ ] **Save Group**: Click "Save"

### Step 4: Create Variable Groups for E-commerce Platform
- [ ] **Create Group**: Click "+ Variable group"
- [ ] **Group Name**: `ecommerce-platform-dev`
- [ ] **Description**: `Development environment for Lab 01AB+01C e-commerce platform`
- [ ] **Add Variable**: `databricks-host` = `your-databricks-workspace-url` (same as above)
- [ ] **Add Variable**: `databricks-token` = `your-copied-token` (same as above)
- [ ] **Secure Token**: Click lock icon 🔒 next to databricks-token
- [ ] **Add Variable**: `azure-subscription-id` = `your-subscription-id`
- [ ] **Add Variable**: `ecommerce-resource-group` = `your-resource-group`
- [ ] **Save Group**: Click "Save"

### Step 5: Verification
- [ ] **Variable Groups Created**: Both `fraud-detection-dev` and `ecommerce-platform-dev` visible
- [ ] **Secure Variables**: Both groups show 🔒 next to databricks-token
- [ ] **Access Test**: Click into each group to verify variables are saved
- [ ] **Token Security**: Original token is saved securely and not displayed in browser

**✅ Authentication Setup Complete**: Variable groups configured for both platforms

---

## 📋 Pipeline Creation Checklist

**Follow this checklist to create CI/CD pipelines for your platforms (25 minutes):**

### Step 1: Create Fraud Detection Pipeline YAML
- [ ] **Create File**: `.azure-pipelines/fraud-detection-pipeline.yml`
- [ ] **Copy Template**: Use the fraud detection pipeline YAML from lesson
- [ ] **Update Paths**: Verify paths match your repository structure:
  - `fraud-detection/l01a-optimized-processing/*`
  - `fraud-detection/l01b-advanced-analytics/*` 
  - `fraud-detection/l01c-integration/*`
- [ ] **Update Variable Group**: Confirm `fraud-detection-dev` is referenced
- [ ] **Commit File**: Commit pipeline YAML to repository

### Step 2: Create E-commerce Pipeline YAML
- [ ] **Create File**: `.azure-pipelines/ecommerce-analytics-pipeline.yml`
- [ ] **Copy Template**: Use the e-commerce pipeline YAML from lesson
- [ ] **Update Paths**: Verify paths match your repository structure:
  - `ecommerce-platform/lab-01ab-analytics/*`
  - `ecommerce-platform/lab-01c-production/*`
- [ ] **Update Variable Group**: Confirm `ecommerce-platform-dev` is referenced
- [ ] **Commit File**: Commit pipeline YAML to repository

### Step 3: Create Pipeline in Azure DevOps (Fraud Detection)
- [ ] **Navigate**: Go to Pipelines → Pipelines → "New pipeline"
- [ ] **Select Source**: Choose "Azure Repos Git"
- [ ] **Select Repository**: Choose your `week-6-data-platforms` repository
- [ ] **Configure Pipeline**: Select "Existing Azure Pipelines YAML file"
- [ ] **Select YAML**: Choose `.azure-pipelines/fraud-detection-pipeline.yml`
- [ ] **Pipeline Name**: Rename to "Fraud Detection Platform (L01A-C)"
- [ ] **Save**: Click "Save" (don't run yet)

### Step 4: Create Pipeline in Azure DevOps (E-commerce)
- [ ] **New Pipeline**: Click "New pipeline" again
- [ ] **Select Source**: Choose "Azure Repos Git"
- [ ] **Select Repository**: Choose your `week-6-data-platforms` repository  
- [ ] **Configure Pipeline**: Select "Existing Azure Pipelines YAML file"
- [ ] **Select YAML**: Choose `.azure-pipelines/ecommerce-analytics-pipeline.yml`
- [ ] **Pipeline Name**: Rename to "E-commerce Analytics Platform (Lab 01AB-C)"
- [ ] **Save**: Click "Save" (don't run yet)

### Step 5: Validate Pipeline Configuration
- [ ] **Fraud Detection Pipeline**: Opens without YAML syntax errors
- [ ] **E-commerce Pipeline**: Opens without YAML syntax errors
- [ ] **Variable Groups**: Both pipelines can access their respective variable groups
- [ ] **Repository Access**: Both pipelines can access repository files
- [ ] **Branch Triggers**: Both pipelines set to trigger on main branch

**✅ Pipeline Creation Complete**: Both platforms have configured CI/CD pipelines

---

## 📋 Testing and Validation Checklist

**Follow this checklist to test your CI/CD pipelines (10 minutes):**

### Step 1: Pre-Flight Validation
- [ ] **Repository Status**: All your completed work is committed to repository
- [ ] **Variable Groups**: Both variable groups accessible and tokens secured
- [ ] **Pipeline YAML**: Both pipeline files validate without syntax errors
- [ ] **Databricks Access**: Can you manually access your Databricks workspace?
- [ ] **Token Validity**: Databricks token is valid and not expired

### Step 2: Test Fraud Detection Pipeline
- [ ] **Manual Trigger**: Go to Fraud Detection Platform pipeline → "Run pipeline"
- [ ] **Monitor Execution**: Watch pipeline stages execute:
  - Validate L01A+L01B+L01C Components
  - Deploy to Databricks
  - Deploy L01C ADF Integration
- [ ] **Check Logs**: Review logs for any warnings or errors
- [ ] **Verify Deployment**: Check Databricks workspace for deployed notebooks:
  - `/fraud-detection/l01a-optimized` folder exists
  - `/fraud-detection/l01b-analytics` folder exists
  - Your L01A and L01B notebooks are visible
- [ ] **Pipeline Status**: Pipeline completes with green checkmark

### Step 3: Test E-commerce Platform Pipeline
- [ ] **Manual Trigger**: Go to E-commerce Analytics Platform pipeline → "Run pipeline"
- [ ] **Monitor Execution**: Watch pipeline stages execute:
  - Validate Lab 01AB + 01C Components
  - Deploy Analytics to Databricks
  - Deploy Lab 01C Production Pipeline
- [ ] **Check Logs**: Review logs for any warnings or errors
- [ ] **Verify Deployment**: Check Databricks workspace for deployed notebooks:
  - `/ecommerce/analytics` folder exists
  - `/ecommerce/production` folder exists
  - Your Lab 01AB analytics notebooks are visible
- [ ] **Pipeline Status**: Pipeline completes with green checkmark

### Step 4: End-to-End Validation
- [ ] **Git Trigger Test**: Make small change to one notebook and commit
- [ ] **Automatic Trigger**: Verify relevant pipeline triggers automatically
- [ ] **Change Deployment**: Confirm change appears in Databricks workspace
- [ ] **Both Platforms**: Both fraud detection and e-commerce platforms deploy successfully
- [ ] **Performance**: Total deployment time < 10 minutes for both platforms

**✅ Testing Complete**: Both platforms deploy automatically and successfully

---

## 📋 Troubleshooting Checklist

**Use this checklist if you encounter issues:**

### Authentication Issues
- [ ] **Token Expiry**: Check if Databricks token has expired (regenerate if needed)
- [ ] **Token Format**: Ensure token is copied completely without extra spaces
- [ ] **URL Format**: Verify Databricks URL includes `https://` prefix
- [ ] **Variable Group Access**: Confirm pipeline has access to variable groups
- [ ] **Secure Variables**: Verify tokens are marked with 🔒 lock icon

### Pipeline Execution Issues
- [ ] **YAML Syntax**: Validate YAML syntax using Azure DevOps validator
- [ ] **File Paths**: Confirm repository file paths match pipeline YAML paths
- [ ] **Branch Name**: Verify you're committing to `main` branch
- [ ] **Repository Permissions**: Confirm pipeline has access to repository
- [ ] **Variable References**: Check variable names match exactly between groups and YAML

### Databricks Deployment Issues
- [ ] **Workspace Access**: Can you manually access Databricks workspace?
- [ ] **CLI Installation**: Check if Databricks CLI installed correctly in pipeline
- [ ] **Authentication Test**: Test token authentication manually if possible
- [ ] **Folder Permissions**: Verify you have permission to create folders in workspace
- [ ] **File Format**: Ensure notebooks are exported as .py files (not .ipynb)

### Performance Issues
- [ ] **Pipeline Timeout**: Check if pipeline is timing out (increase timeout if needed)
- [ ] **Large Files**: Verify notebook files aren't too large (>10MB)
- [ ] **Concurrent Runs**: Check if multiple pipelines running simultaneously
- [ ] **Resource Availability**: Confirm Azure resources are running and available

### Quick Fixes
- [ ] **Restart Pipeline**: Try running pipeline again (sometimes transient issues resolve)
- [ ] **Clear Cache**: Clear browser cache and reload Azure DevOps
- [ ] **Regenerate Token**: Create new Databricks token if authentication fails
- [ ] **Simplify First**: Start with deploying just one notebook to test connectivity
- [ ] **Check Service Status**: Verify Azure DevOps and Databricks service status

**✅ Troubleshooting Complete**: Issues resolved and pipelines working

---

## 📋 Success Validation Checklist

**Use this final checklist to confirm lesson completion:**

### Platform Deployment Success
- [ ] **Fraud Detection Platform**: L01A-C work deploys automatically via CI/CD
- [ ] **E-commerce Platform**: Lab 01AB-C work deploys automatically via CI/CD
- [ ] **Deployment Time**: Both platforms deploy in under 10 minutes
- [ ] **Change Propagation**: Code changes trigger automatic deployment
- [ ] **Error Handling**: Pipelines handle failures gracefully

### Repository Organization
- [ ] **Clean Structure**: Repository follows organized folder structure
- [ ] **Complete Work**: All your L01A-C and Lab 01AB-C work is included
- [ ] **Documentation**: README describes both platforms
- [ ] **Version Control**: All changes properly committed and tracked

### Authentication & Security  
- [ ] **Personal Tokens**: Databricks tokens working for development
- [ ] **Secure Storage**: Sensitive information stored in variable groups with 🔒
- [ ] **Access Control**: Only authorized users can modify pipelines
- [ ] **Token Management**: You know how to rotate tokens when needed

### Technical Knowledge
- [ ] **CI/CD Concepts**: You understand the deployment flow from Git → Azure DevOps → Databricks
- [ ] **Pipeline Structure**: You can explain what each stage of your pipelines does
- [ ] **Troubleshooting**: You can diagnose and fix common pipeline issues
- [ ] **Business Value**: You can articulate how automation improves your platforms

### Learning Outcomes
- [ ] **Automation Confidence**: You're comfortable with automated deployment
- [ ] **Platform Thinking**: You see your L01A-C and Lab work as integrated platforms
- [ ] **DevOps Mindset**: You understand why manual deployment doesn't scale
- [ ] **Production Readiness**: You know what enterprise authentication (Lesson 3B) will add

**✅ Lesson 3A Complete**: Ready to advance to enterprise authentication in Lesson 3B

---

## 📋 Instructor Validation Checklist

**For instructors to verify student success:**

### Student Demonstration Requirements
- [ ] **Live Demo**: Student can trigger both pipelines and show successful deployment
- [ ] **Change Demo**: Student can make code change and show automatic deployment
- [ ] **Platform Explanation**: Student can explain their fraud detection platform (L01A-C)  
- [ ] **Platform Explanation**: Student can explain their e-commerce platform (Lab 01AB-C)
- [ ] **Troubleshooting**: Student can diagnose a provided authentication failure

### Technical Verification
- [ ] **Repository Check**: Student's repository contains their actual completed work
- [ ] **Pipeline Check**: Both pipelines execute without errors
- [ ] **Databricks Check**: Notebooks appear correctly in student's Databricks workspace
- [ ] **Variable Groups**: Variable groups properly configured with secure tokens
- [ ] **Performance**: Deployment completes within reasonable time

### Knowledge Assessment
- [ ] **DevOps Value**: Student can explain business benefits of automation
- [ ] **Platform Integration**: Student understands how their L01A-C work connects
- [ ] **Lab Integration**: Student understands how their Lab 01AB-C work connects
- [ ] **Next Steps**: Student can identify what Lesson 3B enterprise authentication will add

**✅ Student Ready for Lesson 3B**: All validation criteria met