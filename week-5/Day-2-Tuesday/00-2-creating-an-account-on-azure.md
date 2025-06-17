# Creating an Azure Account and Data Factory Setup

**Duration:** 25 minutes  

## Introduction

**"Setting Up Your Data Engineering Command Center"**

Remember the first time you set up your development environment? Installing Java, configuring your IDE, setting up version control, connecting to databases – it felt like you were building a professional toolkit. Each tool you configured was a step toward becoming a real developer.

Today, you're taking that same step into data engineering. But instead of setting up IntelliJ and MySQL on your laptop, you're configuring enterprise-grade cloud services that Fortune 500 companies use to process billions of dollars in transactions daily.

**What You're Actually Building:**
In the next 25 minutes, you'll create your own instance of the same data integration platform that:
- **JP Morgan Chase** uses to process 6 billion transactions daily
- **Walmart** uses to analyze inventory across 10,500 stores worldwide  
- **Johnson & Johnson** uses to track pharmaceutical supply chains globally
- **Your future employer** is probably implementing right now

**This Isn't Just Account Setup:**
Think of this as provisioning your professional data engineering workspace. Every service you configure today becomes a tool in your arsenal – the same tools that data engineers use to command $75,000-$125,000+ salaries.

**The Professional Difference:**
While other bootcamp graduates are still running code on their laptops, you're about to have access to enterprise cloud infrastructure. When you tell an interviewer "I have hands-on experience with Azure Data Factory and enterprise data pipelines," you're speaking their language.

**What Happens Next:**
Once your Azure environment is configured, you'll have access to the same data processing capabilities as major enterprises. The pipelines you build in upcoming lessons will be production-ready, scalable, and exactly the type of work that gets data engineers hired.

**Your Mission:**
Transform from a developer who processes data locally to a data engineer who orchestrates enterprise-scale data operations in the cloud. Your Azure Data Factory instance is your command center for this transformation.

Ready to set up your professional data engineering environment? Let's get your cloud infrastructure running.

## Learning Outcomes

By the end of this lesson, students will be able to:
- Create a free Azure account with data services access
- Set up Azure Data Factory workspace
- Navigate Azure data services in the portal
- Understand cost management for Azure data services

## Prerequisites
- Valid email address
- Credit card (for verification, no charges during free trial)

---

## Lesson Content

### Opening (3 minutes)

#### Azure Free Tier for Data Services

Azure provides an excellent free tier specifically designed for learning and development work. For data engineering, this is particularly generous and will cover everything we need for this course.

**What you get for free with Azure data services:**

**Azure Data Factory:**
- **5 data integration activities** per month (enough for our course projects)
- **1 pipeline** with scheduling and monitoring
- **Free tier includes:** Copy activities, data flows, and pipeline orchestration

**Azure Databricks:**
- **14-day free trial** of Databricks Premium
- **After trial:** Continue with Standard tier (pay-as-you-go, but very affordable for learning)
- **What this covers:** Cluster time, notebook development, collaborative features

**Other Data Services:**
- **Power BI:** Free individual license for personal use and learning
- **Azure Storage:** 5 GB locally redundant storage (enough for course datasets)
- **Azure SQL Database:** 100 DTU hours monthly + 250 GB storage

**Plus the general Azure free tier:**
- **$200 credit** for first 30 days to explore paid services
- **Always free services** for ongoing development work

#### What to Expect During Setup

The setup process will take about 20 minutes and involves:

1. **Azure Account Creation** (5 minutes) - Same process as other cloud services
2. **Data Factory Setup** (10 minutes) - Create your first data integration workspace
3. **Portal Navigation** (5 minutes) - Learn to find and access data services efficiently

**Important for Data Engineers:** Unlike general Azure development, data services require specific configuration for optimal performance and cost management. We'll set up everything correctly from the start.

### Hands-on Setup (20 minutes)

#### Create Azure Account (5 minutes)

This is the same process as the previous lesson, but I'll highlight the data-specific considerations:

**Step 1: Navigate to Azure Portal**
1. Go to **portal.azure.com**
2. Click **"Start free"** or **"Create free account"**

**Step 2: Account Creation**
1. Sign in with existing Microsoft account or create new one
2. **For data engineering:** Use a professional email if possible (helps with later enterprise features)
3. Complete phone verification
4. Provide payment method for verification

**Step 3: Verify Data Services Access**
Once your account is active, verify you can access data services:

```
Search for these services in the Azure portal:
✓ Azure Data Factory
✓ Azure Databricks  
✓ Power BI
✓ Azure Storage Account
✓ Azure SQL Database
```

If any service shows "not available" or requires additional signup, we'll troubleshoot together.

#### Set up Azure Data Factory (10 minutes)

Azure Data Factory will be our orchestration hub, so let's set it up properly.

**Step 1: Create Data Factory Instance**

1. **Search** for "Data Factory" in the Azure portal
2. **Click** "Create Data Factory"
3. **Configure the basics:**

```
Subscription: Azure for Students (or Free Trial)
Resource Group: Create new "DataEngineering-Course"
Name: [YourName]-data-factory (must be globally unique)
Version: V2 (default)
Region: East US (or closest to you)
```

4. **Click** "Review + Create" then "Create"
5. **Wait** for deployment (2-3 minutes)

**Step 2: Launch Data Factory Studio**

1. **Go to resource** after deployment completes
2. **Click** "Launch studio" button
3. **New tab opens** with Data Factory Studio interface

**Step 3: Explore Data Factory Studio Interface**

**Author Tab:** Where you'll build data pipelines
- **Pipelines:** Orchestration workflows
- **Datasets:** Data source and destination definitions
- **Data flows:** Visual data transformation designer
- **Linked services:** Connections to data sources

**Monitor Tab:** Track pipeline runs and performance
- **Pipeline runs:** Execution history and status
- **Activity runs:** Detailed activity-level monitoring
- **Alerts:** Set up notifications for failures or performance issues

**Manage Tab:** Configuration and security
- **Linked services:** Database connections, API credentials
- **Integration runtimes:** Compute environments for data processing
- **Triggers:** Scheduling and event-based execution

**Let's Create a Simple Test:**

1. **Click** "Author" tab
2. **Click** "+" to create new pipeline
3. **Name it** "Test-Pipeline"
4. **Drag** a "Copy data" activity from the toolbox
5. **Don't configure it yet** - just verify the interface works
6. **Save** the pipeline

**Why This Matters:** Data Factory will orchestrate all our data movement throughout the course. Understanding the interface now will make later lessons much smoother.

#### Configure Resource Groups for Data Projects (3 minutes)

Resource groups are crucial for managing Azure data projects efficiently.

**Create Organized Resource Structure:**

1. **Navigate** to "Resource groups" in Azure portal
2. **Verify** "DataEngineering-Course" resource group exists
3. **Click** into the resource group
4. **You should see** your Data Factory instance listed

**Resource Group Best Practices for Data Engineering:**

```
DataEngineering-Course/
├── [YourName]-data-factory          (orchestration)
├── [YourName]-databricks-workspace  (processing) - we'll create this in L01
├── [YourName]-storage-account       (data lake) - we'll create this in L01  
├── [YourName]-cosmosdb-account      (document data) - we'll create this in L04
└── [YourName]-sql-server            (relational data) - we'll create this in L03
```

**Why Resource Groups Matter:**
- **Cost Management:** See spending for entire project at once
- **Security:** Apply permissions to entire project
- **Cleanup:** Delete entire project easily when done
- **Organization:** Keep related services together

**Set up Cost Monitoring:**

1. **Go to** your resource group
2. **Click** "Cost analysis" in left sidebar
3. **Click** "Create budget"
4. **Configure budget:**
   - Name: "Course Budget"
   - Amount: $50 (conservative limit)
   - Alert at 80% ($40)
   - Send to your email
5. **Save** the budget

#### Verify Access to Other Data Services (2 minutes)

Let's confirm you can access the other Azure data services we'll use:

**Azure Storage Account Access:**
1. **Search** "Storage accounts"
2. **Click** "Create storage account"
3. **Don't create it** - just verify you can access the creation form
4. **Cancel** out of the creation process

**Azure Databricks Access:**
1. **Search** "Azure Databricks"
2. **Click** "Create Azure Databricks Service"
3. **Verify** you can see the creation form
4. **Cancel** out - we'll create this in L01

**Power BI Access:**
1. **Open new tab** and go to **powerbi.microsoft.com**
2. **Sign in** with your Azure account
3. **You should see** the Power BI service interface
4. **If prompted** to start trial, you can do so (it's free)

**Azure SQL Database Access:**
1. **Search** "SQL databases" in Azure portal
2. **Click** "Create SQL database"
3. **Verify** you can see pricing tiers and configuration options
4. **Cancel** out - we'll create this in L03

### Verification & Troubleshooting (2 minutes)

#### Confirm Data Factory Access

Let's verify your Data Factory setup is working correctly:

**Basic Functionality Test:**
1. **Go back** to Data Factory Studio
2. **Click** "Author" tab
3. **Open** the "Test-Pipeline" you created earlier
4. **Click** "Debug" button (this tests the pipeline without saving)
5. **You should see** a message about missing source/sink (this is expected)
6. **This confirms** the interface is working correctly

**Connection Test:**
1. **Click** "Manage" tab in Data Factory Studio
2. **Click** "Linked services"
3. **Click** "+ New" to create a new linked service
4. **Browse** available connectors - you should see options for:
   - Azure SQL Database
   - Azure Blob Storage
   - Azure Cosmos DB
   - Many others
5. **Cancel** out - we'll create actual connections in later lessons

#### Common Setup Issues and Solutions

**Issue 1: Data Factory creation fails**
- **Symptom:** Error during Data Factory deployment
- **Common causes:** Name not globally unique, region not available
- **Solution:** Try different name, select different region (West US, North Europe)

**Issue 2: Can't access Data Factory Studio**
- **Symptom:** "Launch studio" button doesn't work or shows error
- **Solution:** Try different browser, disable ad blockers, clear browser cache

**Issue 3: Resource group permissions**
- **Symptom:** Can't create resources in resource group
- **Solution:** Verify subscription is active, check that you're using correct subscription

**Issue 4: Budget alerts not working**
- **Symptom:** Not receiving cost alert emails
- **Solution:** Check spam folder, verify email in Azure profile settings

#### Set up Cost Monitoring for Data Services

**Advanced Cost Management Setup:**

1. **Navigate** to "Cost Management + Billing"
2. **Click** "Cost analysis"
3. **Filter by** resource group "DataEngineering-Course"
4. **Set up custom view:**
   - Group by: Service name
   - Chart type: Stacked column
   - Time range: Last 30 days

**Create Service-Specific Alerts:**

```
Alert 1: Data Factory spending > $10/month
Alert 2: Databricks spending > $20/month  
Alert 3: Total resource group > $50/month
```

**Why This Matters:** Data services can scale automatically, which is great for performance but can lead to unexpected costs. Monitoring from day one builds good habits.

**Pro Tips for Cost Management:**
- **Always set auto-termination** on Databricks clusters
- **Use scheduled triggers** in Data Factory instead of continuous monitoring
- **Delete test resources** immediately after each lesson
- **Monitor daily during learning** to understand usage patterns

---

## Materials Needed

### Azure Data Factory Setup Guide

**Pre-Setup Checklist:**
- [ ] Azure account created and verified
- [ ] Resource group "DataEngineering-Course" created
- [ ] Data Factory instance successfully deployed
- [ ] Data Factory Studio accessible
- [ ] Test pipeline created and debugged

**Configuration Details:**
```
Data Factory Configuration:
├── Name: [YourName]-data-factory
├── Resource Group: DataEngineering-Course
├── Region: East US (or nearest)
├── Version: V2
├── Git Configuration: Skip for now (we'll add later)
└── Managed Identity: Auto-created
```

**Interface Navigation Reference:**
```
Data Factory Studio Tabs:
├── Home: Quick access to common tasks
├── Author: Build pipelines, datasets, data flows
├── Monitor: Track execution and performance  
├── Manage: Configure connections and settings
└── Learn: Tutorials and documentation
```

### Cost Management Configuration Steps

**Budget Setup:**
1. **Resource Group Budget:**
   - Name: "Course Budget"
   - Amount: $50
   - Period: Monthly
   - Alert thresholds: 50%, 80%, 100%

2. **Service-Specific Monitoring:**
   - Data Factory: Track pipeline runs and data movement
   - Databricks: Monitor cluster uptime and compute costs
   - Storage: Track data storage and transaction costs

**Cost Optimization Checklist:**
- [ ] Auto-termination configured for all compute resources
- [ ] Budget alerts set up with email notifications
- [ ] Daily cost monitoring scheduled
- [ ] Unused resources deleted after each lesson

### Troubleshooting Guide for Data Services

**Common Issues and Solutions:**

| Issue | Symptom | Solution |
|-------|---------|----------|
| Data Factory Studio won't load | Blank page or error message | Try incognito mode, different browser, check pop-up blockers |
| Can't create linked services | "Access denied" errors | Verify permissions, check subscription status |
| Resource group access issues | Can't see or modify resources | Confirm you're in correct subscription, check user role |
| Cost alerts not working | No email notifications | Verify email in profile, check spam folder, confirm alert rules |

**Browser Requirements:**
- **Recommended:** Chrome, Edge, Firefox (latest versions)
- **Required:** JavaScript enabled, pop-ups allowed for Azure domains
- **Avoid:** Internet Explorer (not supported for Data Factory Studio)

**Network Requirements:**
- **Outbound HTTPS:** Required for Azure services communication
- **Corporate Networks:** May need firewall exceptions for Azure domains
- **VPN Issues:** Some VPNs may interfere with Azure portal access

