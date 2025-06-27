# Azure DevOps & ADF ARM Template Deployment - 2-Hour Workshop

## Workshop Overview
**Duration:** 2 hours  
**Level:** Intermediate  
**Prerequisites:** Basic Azure familiarity, existing Azure Data Factory with at least one pipeline

**⚠️ Important Note:** Due to Microsoft's parallelism policy, pipelines will fail during the workshop. Students will need to request approval from Microsoft and re-run pipelines after the workshop (1-3 business days).

## Learning Objectives
Students will learn to:
- Set up Azure DevOps organization, project, and repository
- Export ARM templates from Azure Data Factory
- Create YAML deployment pipelines
- Implement basic CI/CD for ADF resources
- Understand the distinction between data orchestration and infrastructure deployment

---

## Module 1: DevOps Setup (25 minutes)

### 1.1 Create Azure DevOps Organization (8 minutes)
**Quick Setup Steps:**
1. Navigate to https://dev.azure.com
2. Sign in with Azure account
3. Click "New organization"
4. Name: `[YourName]-ADF-Workshop`
5. Choose region closest to your location
6. Accept defaults and create

### 1.2 Create Project (7 minutes)
1. Click "New Project"
2. Project name: `ADF-Deployment`
3. Visibility: Private
4. Version control: Git
5. Work item process: Basic
6. Click "Create"

### 1.3 Initialize Repository (5 minutes)
1. Navigate to "Repos" → "Files"
2. Click "Initialize" with README
3. Create folder structure:
   ```
   /templates
   /pipelines
   /parameters
   ```

### 1.4 Understanding Two Types of Pipelines (5 minutes)
**This is a critical concept that often confuses students:**

**Azure Data Factory Pipelines** = **Data Orchestration**
- **Purpose**: Move, transform, and process data
- **Components**: Activities like Copy Data, Data Flow, Execute Pipeline
- **Triggers**: Schedule-based, event-based, or manual
- **Example**: "Every night at 2 AM, copy data from SQL Server to Data Lake, then run analytics"

**Azure DevOps Pipelines** = **Infrastructure & Code Deployment**
- **Purpose**: Deploy infrastructure, applications, and configurations
- **Components**: Build steps, deployment tasks, environment management  
- **Triggers**: Code commits, pull requests, manual releases
- **Example**: "When I commit ADF changes, deploy the updated Data Factory to production"

**Key Relationship**: 
- **ADF Pipelines** = What your data workflows DO
- **DevOps Pipelines** = How you DEPLOY those data workflows

**Real-World Scenario**: You build a data pipeline in ADF that processes customer orders every hour. You use a DevOps pipeline to automatically deploy that ADF pipeline (and any updates) across development, test, and production environments.

---

## Module 2: Export ADF ARM Template (25 minutes)

### 2.1 Access Your Data Factory (5 minutes)
1. Open Azure Portal
2. Navigate to your existing Azure Data Factory
3. Click "Open Azure Data Factory Studio"
4. Ensure you have at least one pipeline created

### 2.2 Export ARM Template (15 minutes)
**Step-by-Step Export:**
1. In ADF Studio, click "Manage" hub (toolbox icon)
2. Under "Source control", select "ARM template"
3. Click "Export ARM template"
4. **Download the ZIP file** containing:
   - `ARMTemplateForFactory.json` (main template)
   - `ARMTemplateParametersForFactory.json` (parameters)
   - `globalParameters.json` (if applicable)

### 2.3 Examine Template Structure (5 minutes)
**Key Components to Understand:**
- **ARMTemplateForFactory.json**: Contains all ADF resources (pipelines, datasets, linked services)
- **ARMTemplateParametersForFactory.json**: Environment-specific values

**What is an ARM Template?**
Think of ARM templates as "blueprints" for your Azure resources. Just like architectural blueprints describe how to build a house, ARM templates describe how to build your Azure Data Factory.

**Why Use ARM Templates for ADF?**
- **Consistency**: Deploy identical ADF structures across environments  
- **Version Control**: Track changes to your data infrastructure over time
- **Automation**: Deploy complex ADF setups with a single command
- **Environment Management**: Same ADF logic, different connection strings per environment

**The Magic of Parameters**: 
The template describes WHAT to build (pipelines, datasets), while parameters describe WHERE and HOW (connection strings, storage accounts, environment-specific settings). This separation allows one template to work across development, test, and production environments.

---

## Module 3: Create YAML Pipeline (30 minutes)

### 3.1 Upload ARM Templates to Repo (8 minutes)
1. In Azure DevOps, go to "Repos" → "Files"
2. Navigate to `/templates` folder
3. Click "Upload files"
4. Upload both JSON files from the exported ZIP
5. Commit with message: "Add ADF ARM templates"

### 3.2 Create Parameters File (7 minutes)
Create `/parameters/dev-parameters.json`:
```json
{
  "$schema": "https://schema.management.azure.com/schemas/2015-01-01/deploymentParameters.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
    "factoryName": {
      "value": "your-adf-name-dev"
    },
    "location": {
      "value": "East US"
    }
  }
}
```

**Understanding the CI/CD Workflow:**
This step illustrates a fundamental DevOps concept that students often find abstract:

**Traditional Approach (Manual)**:
1. Build ADF pipelines in development
2. Manually recreate them in production  
3. Manually update connection strings
4. Hope nothing breaks!

**CI/CD Approach (Automated)**:
1. **ADF Orchestrates**: Your data pipelines run automatically (move data, run analytics)
2. **DevOps Deploys**: Infrastructure changes deploy automatically (new pipelines, updated connections)

**The Key Insight**: 
- **ADF is your RUNTIME** - it executes data workflows
- **DevOps is your DEPLOYMENT ENGINE** - it manages how those workflows get deployed

**Real-World Example**: 
Your ADF pipeline processes sales data every morning at 6 AM (orchestration). When your team adds a new data source, the DevOps pipeline automatically deploys the updated ADF configuration to production that same day (deployment). The sales data processing continues running seamlessly with the new data source.

### 3.3 Create Basic YAML Pipeline (15 minutes)
Create `/pipelines/adf-deployment.yml`:

**⚠️ IMPORTANT: Students must update the following values with their actual Azure information:**

```yaml
trigger:
- main

pool:
  vmImage: 'ubuntu-latest'

variables:
  resourceGroupName: 'rg-adf-workshop'           # UPDATE: Your target resource group name
  location: 'East US'                            # UPDATE: Your Azure region
  serviceConnection: 'Azure-Service-Connection'  # UPDATE: Match your service connection name

stages:
- stage: DeployADF
  displayName: 'Deploy Azure Data Factory'
  jobs:
  - job: Deploy
    displayName: 'Deploy ADF Resources'
    steps:
    
    - task: AzureResourceManagerTemplateDeployment@3
      displayName: 'Deploy ADF ARM Template'
      inputs:
        deploymentScope: 'Resource Group'
        azureResourceManagerConnection: '$(serviceConnection)'
        subscriptionId: 'your-subscription-id'                    # ⚠️ MUST UPDATE: Your Azure subscription ID
        action: 'Create Or Update Resource Group'
        resourceGroupName: '$(resourceGroupName)'
        location: '$(location)'
        templateLocation: 'Linked artifact'
        csmFile: '$(Build.SourcesDirectory)/templates/ARMTemplateForFactory.json'
        csmParametersFile: '$(Build.SourcesDirectory)/parameters/dev-parameters.json'
        deploymentMode: 'Incremental'
        
    - task: PublishBuildArtifacts@1
      displayName: 'Publish Artifacts'
      inputs:
        PathtoPublish: '$(Build.SourcesDirectory)'
        ArtifactName: 'ADF-Templates'
```

**📋 Before running the pipeline, students MUST customize these values:**

1. **subscriptionId**: Replace `'your-subscription-id'` with your actual Azure subscription ID
   - Find this in Azure Portal → Subscriptions → Copy the Subscription ID
   - Example: `'12345678-1234-1234-1234-123456789012'`

2. **resourceGroupName**: Update the variable to match your target resource group
   - If it doesn't exist, the pipeline will create it
   - Example: `'rg-student-adf-demo'`

3. **location**: Set to your preferred Azure region
   - Must match where your original ADF is located
   - Example: `'West US 2'`, `'North Europe'`

4. **serviceConnection**: Must match the name you created in the previous step
   - Go to Project Settings → Service Connections to verify the exact name

**Understanding the YAML Structure:**
The YAML pipeline might look complex, but it follows a logical pattern:

**Trigger Section**: `trigger: - main`
- "When code changes on the main branch, automatically run this pipeline"
- This enables Continuous Integration (CI)

**Pool Section**: `pool: vmImage: 'ubuntu-latest'`  
- "Use Microsoft's hosted Ubuntu virtual machine to run the deployment"
- This is where the parallelism policy limitation occurs

**Variables Section**: 
- Think of these as "settings" that can be reused throughout the pipeline
- Easier to maintain than hardcoding values everywhere

**AzureResourceManagerTemplateDeployment Task**:
- This is the "magic" task that takes your ARM template and creates actual Azure resources
- It's like saying "Azure, here's my blueprint (ARM template), please build it"
- The task handles authentication, resource creation, and error reporting

**Why This Approach Works**:
- **Repeatability**: Same deployment process every time
- **Traceability**: Full log of what was deployed when
- **Rollback Capability**: Can revert to previous template versions
- **Environment Consistency**: Same process for dev, test, and production

---

## Module 4: Pipeline Configuration & Testing (35 minutes)

### 4.1 Create Service Connection (12 minutes)
1. Go to "Project Settings" → "Service connections"
2. Click "New service connection"
3. Select "Azure Resource Manager"
4. Choose "Service principal (automatic)"
5. Select subscription and resource group
6. Name: `Azure-Service-Connection`
7. Grant permissions and save

**Understanding Service Connections:**
Service connections often confuse students, so let's clarify:

**What is a Service Connection?**
A service connection is like a "secure key" that allows Azure DevOps to access your Azure subscription. Without it, DevOps pipelines can't create or modify Azure resources.

**Why Not Use Your Personal Account?**
- Personal accounts shouldn't be used for automated processes
- Service principals provide specific, limited permissions
- If you leave the organization, the automation continues working
- Better security through principle of least privilege

**The "Automatic" Option**:
- Azure DevOps creates a service principal for you behind the scenes
- It gets just enough permissions to manage resources in the specified resource group
- This is the easiest and most secure approach for learning environments

### 4.2 Update Pipeline Variables (8 minutes)
1. Edit the YAML file with your actual values:
   - `subscriptionId`: Your Azure subscription ID
   - `resourceGroupName`: Target resource group
   - Update parameters file with correct ADF name

### 4.3 Create and Run Pipeline (10 minutes)

**🚨 CRITICAL NOTE: Microsoft Parallelism Policy**

**Your pipeline will likely FAIL with this error:**
```
##[error]No hosted parallelism has been purchased or granted. 
To request a free parallelism grant, please fill out the following form: 
https://aka.ms/azpipelines-parallelism-request
```

**Why This Happens:**
Microsoft changed their policy in 2021. New Azure DevOps organizations no longer receive free hosted parallelism automatically. This affects all pipelines using Microsoft-hosted agents (like `vmImage: 'ubuntu-latest'`).

**What We'll Do:**
1. Create the pipeline (it will fail as expected)
2. Submit a parallelism request to Microsoft
3. Wait for approval (typically 1-3 business days)
4. Re-run the pipeline once approved

**Steps to Create Pipeline:**
1. Go to "Pipelines" → "Pipelines"
2. Click "New pipeline"
3. Select "Azure Repos Git"
4. Choose your repository
5. Select "Existing Azure Pipelines YAML file"
6. Path: `/pipelines/adf-deployment.yml`
7. Click "Run" (expect it to fail with parallelism error)

**Request Parallelism Approval:**
1. When the pipeline fails, click the link in the error message: https://aka.ms/azpipelines-parallelism-request
2. Fill out the form with:
   - **Organization**: Your Azure DevOps organization name
   - **Project**: Your project name  
   - **Business justification**: "Educational/Learning purposes - DevOps workshop"
3. Submit and wait for Microsoft's approval email

### 4.4 Monitor Pipeline Execution (5 minutes)
- Watch pipeline stages execute
- Check logs for any errors
- Verify ADF resources deployed in Azure Portal

---

## Module 5: Best Practices & Troubleshooting (15 minutes)

### Best Practices

**Repository Structure:**
```
/templates/          # ARM templates
/parameters/         # Environment-specific parameters
  ├── dev-parameters.json
  ├── test-parameters.json
  └── prod-parameters.json
/pipelines/          # YAML pipeline definitions
/scripts/            # PowerShell/CLI scripts
```

**Security:**
- Use service connections, never hardcode credentials
- Store sensitive values in Azure Key Vault
- Reference Key Vault in parameter files:
```json
"connectionString": {
  "reference": {
    "keyVault": {
      "id": "/subscriptions/.../Microsoft.KeyVault/vaults/myvault"
    },
    "secretName": "connectionString"
  }
}
```

**Deployment Strategy:**
- Use incremental deployment mode for safety
- Always validate templates before deployment
- Implement proper approval gates for production
- Use environment-specific parameter files

### Common Issues & Solutions

**Microsoft Parallelism Policy (Most Common Issue):**
- **Error**: "No hosted parallelism has been purchased or granted"
- **Solution**: Submit request at https://aka.ms/azpipelines-parallelism-request
- **Timeline**: 1-3 business days for approval
- **Alternative**: Use self-hosted agents (advanced setup required)

**Pipeline Failures:**
- **Service Connection Issues**: Verify permissions and subscription access
- **ARM Template Errors**: Use validation deployment mode first
- **Parameter Mismatches**: Ensure parameter files match template requirements
- **Resource Dependencies**: Check deployment order in template

**ADF-Specific Issues:**
- **Trigger Management**: Stop triggers before deployment, restart after
- **Linked Service Connections**: Update connection strings per environment
- **Dataset References**: Ensure parameter values match target environment

**Conceptual Misunderstandings (Common Student Questions):**

**"Why can't I just copy-paste my ADF pipelines?"**
- Manual copying doesn't scale and introduces human error
- ARM templates ensure exact replication across environments
- Version control tracks what changed and when

**"When does my ADF pipeline actually run data processing?"**
- ADF pipelines run independently of DevOps pipelines
- DevOps deploys the ADF infrastructure, then ADF schedules and runs data workflows
- Think: DevOps builds the factory, ADF operates the factory

**"Why do I need both ARM templates AND parameter files?"**
- ARM templates = the "recipe" (same across all environments)
- Parameter files = the "ingredients" (different per environment)
- One recipe, different ingredients = consistent results in different kitchens

**Quick Fixes:**
```yaml
# Add trigger management
- task: AzurePowerShell@5
  displayName: 'Stop ADF Triggers'
  inputs:
    azureSubscription: '$(serviceConnection)'
    ScriptType: 'InlineScript'
    Inline: |
      $triggers = Get-AzDataFactoryV2Trigger -ResourceGroupName "$(resourceGroupName)" -DataFactoryName "$(factoryName)"
      foreach($trigger in $triggers) {
        Stop-AzDataFactoryV2Trigger -ResourceGroupName "$(resourceGroupName)" -DataFactoryName "$(factoryName)" -Name $trigger.Name -Force
      }
```

### Validation Checklist
- [ ] ARM templates export successfully
- [ ] Parameter files contain correct values
- [ ] Service connection has required permissions
- [ ] Pipeline YAML syntax is valid
- [ ] Resource group exists in target subscription
- [ ] All dependencies are included in template

---

## Wrap-up & Next Steps (10 minutes)

### What We Accomplished
- Set up complete Azure DevOps organization, project, and repository
- Exported ARM templates from Azure Data Factory
- Created YAML CI/CD pipeline for ADF deployment
- Learned about Microsoft's parallelism policy and approval process
- Implemented environment-specific configurations
- **Understood the critical distinction**: ADF orchestrates data workflows, DevOps deploys the infrastructure that runs those workflows

### Key Conceptual Takeaways
**Two-Pipeline Architecture**:
- **ADF Pipelines**: "What work gets done" (data processing, transformation)
- **DevOps Pipelines**: "How work gets deployed" (infrastructure, configuration)

**ARM Template Power**:
- Templates define the "what" (infrastructure blueprint)
- Parameters define the "where" (environment-specific values)
- Together they enable consistent, repeatable deployments

**CI/CD for Data Platforms**:
- Traditional CI/CD focuses on application code
- Data platform CI/CD focuses on infrastructure and configuration
- Both work together to create reliable, scalable data solutions

### Expected Outcome
**During Workshop**: Pipeline creation successful, but execution will fail due to parallelism policy
**Post-Workshop**: After Microsoft approval (1-3 days), students can re-run pipelines successfully

### Immediate Next Steps for Students
1. **Submit Parallelism Request**: Complete the form at https://aka.ms/azpipelines-parallelism-request
2. **Wait for Approval**: Monitor email for Microsoft's approval notification
3. **Test Pipeline**: Re-run pipeline once parallelism is granted
4. **Verify Deployment**: Check Azure Portal to confirm ADF resources deployed

### Future Enhancements
1. **Add Validation**: Include ARM template validation before deployment
2. **Multi-Environment**: Set up staging and production environments  
3. **Monitoring**: Integrate Azure Monitor and alerting
4. **Advanced Features**: Explore pipeline templates and variable groups

### Resources for Continued Learning
- [Microsoft Learn: ADF CI/CD](https://learn.microsoft.com/en-us/azure/data-factory/continuous-integration-delivery)
- [Azure DevOps YAML Schema Reference](https://learn.microsoft.com/en-us/azure/devops/pipelines/yaml-schema)
- [ARM Template Best Practices](https://learn.microsoft.com/en-us/azure/azure-resource-manager/templates/best-practices)

---

## Quick Reference Commands

**Export ADF Template via PowerShell:**
```powershell
Export-AzDataFactoryV2 -ResourceGroupName "myRG" -DataFactoryName "myADF" -Path "C:\temp\adf-export"
```

**Validate ARM Template:**
```bash
az deployment group validate --resource-group myRG --template-file template.json --parameters @parameters.json
```

**Basic Pipeline Structure:**
```yaml
trigger: [main]
pool: { vmImage: 'ubuntu-latest' }
steps:
- task: AzureResourceManagerTemplateDeployment@3
  inputs:
    deploymentScope: 'Resource Group'
    # ... configuration
```