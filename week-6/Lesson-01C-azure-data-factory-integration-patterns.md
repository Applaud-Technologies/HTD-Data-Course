# L01C: Azure Data Factory Integration Patterns

**Duration:** 120 minutes (2 hours)



## Introduction

**"The difference between a data engineer who builds one-off scripts and one who builds enterprise data platforms is understanding orchestration, error handling, and data integration patterns."**

In L01A and L01B, you transformed your individual PySpark jobs and SparkSQL queries from basic functional code into optimized, production-ready components. You now have a robust fraud detection pipeline with proper error handling and performance optimization. But here's the enterprise reality: **individual components, no matter how well-optimized, don't create production data platforms.**

Production data engineering requires orchestrating multiple optimized components, handling failures gracefully across the entire workflow, and integrating disparate systems reliably. This is where Azure Data Factory transforms your collection of optimized processing jobs into an enterprise-grade data platform.

**Building on Your L01A & L01B Foundation:**

- ✅ You have optimized PySpark jobs with proper error handling (L01A)
- ✅ You have efficient SparkSQL queries for complex analytics (L01B)
- ✅ You understand performance optimization and resource management
- 🎯 **Today's Goal:** Orchestrate these components into an integrated platform

**What You're About to Master:**
Today, you'll evolve from building individual optimized data processing jobs to designing integrated data pipelines that orchestrate your fraud detection components, handle errors systematically across the entire workflow, and scale with enterprise requirements.

**Your Journey Today:**

- **Orchestrate**: Your optimized L01A/L01B components into cohesive workflows (30 min)
- **Secure**: Production error handling and retry logic across multi-component pipelines (30 min)
- **Practice**: Three focused exercises to build your complete integrated platform (45 min)
- **Deploy**: Preparation for L03 CI/CD automation (15 min)

**The Challenge:**
By the end of today's lesson, you'll have built a production-ready data integration platform that orchestrates your optimized fraud detection components from L01A and L01B, processes banking data from multiple sources reliably, and delivers results to analytical systems—transforming individual excellence into platform excellence.

Ready to transform from component optimizer to platform architect? Let's orchestrate your optimized components into enterprise systems.



## Learning Outcomes

By the end of this lesson, students will be able to:

- Design and implement robust Azure Data Factory pipelines with comprehensive error handling
- Create parameterized, reusable workflows that adapt to multiple data sources and schedules
- Implement monitoring, logging, and alerting systems for production data integration
- Optimize ADF pipeline performance and cost management for enterprise-scale workloads
- Integrate ADF with Azure Databricks for end-to-end data processing workflows



## Prerequisites

- Completion of L01A: Azure Databricks Deep Dive Review
- Completion of L01B: SparkSQL Mastery Workshop
- Understanding of basic Azure Data Factory concepts from Week 5
- Access to Azure subscription with Data Factory and Databricks resources

---



> **NOTE:**
>
> **Your previous notebooks for Lessons 01A and 01B should be renamed to the following to complete this pipeline configuration:**
>
> - `Lesson-01A-Azure-Databricks-Review.ipynb`
> - `Lesson-01B-SparkSQL-Mastery-Workshop-Solution.ipynb`



---



## Section 1: Build Your Integrated Pipeline Foundation (30 minutes)



### Step 1: Create Your Basic Pipeline Structure

**What You're Building:**
An Azure Data Factory pipeline that runs your L01A fraud detection notebook first, then automatically runs your L01B analytics notebook on the results. This creates an integrated fraud detection platform that processes banking data from start to finish.

**UI Steps:**
1. Open Azure Data Factory Studio in your browser
2. Navigate to **Author** (pencil icon) → **Pipelines** → **New Pipeline**
3. Name your pipeline: `fraud-detection-integrated-pipeline`
4. Add a description: `Orchestrates L01A PySpark and L01B SparkSQL components for complete fraud detection`

**Behind the Scenes:**
These UI actions create the basic pipeline structure in JSON:
```json
{
  "name": "fraud-detection-integrated-pipeline",
  "properties": {
    "description": "Orchestrates L01A PySpark and L01B SparkSQL components for complete fraud detection",
    "activities": [],
    "parameters": {}
  }
}
```

**Why This Matters:**
- The pipeline name helps you identify this workflow in monitoring and logs
- The description documents the business purpose for other team members
- The empty activities array will hold your L01A and L01B processing steps
- Parameters will allow you to reuse this pipeline with different data sources



### Step 2: Add Your L01A Processing Activity

**What You're Building:**
The first step in your pipeline that runs your optimized L01A fraud detection notebook from the previous lesson.

**UI Steps:**
1. From the **Activities** panel, expand **Databricks**
2. Drag **Databricks Notebook** onto the pipeline canvas
3. Select the activity and configure in the **General** tab:
   - **Name**: `ExecuteL01A_OptimizedProcessing`
   - **Description**: `Run L01A fraud detection with optimized PySpark`
   - **Timeout**: `1:00:00` (1 hour)
4. In the **Azure Databricks** tab:
   - Select your Databricks linked service
   - **Notebook path**: `/Notebooks/Lesson-01A-Azure-Databricks-Review`

**Behind the Scenes:**
Your UI configuration generates this activity definition:

```json
{
  "name": "ExecuteL01A_OptimizedProcessing",
  "type": "DatabricksNotebook",
  "typeProperties": {
    "notebookPath": "/Notebooks/Lesson-01A-Azure-Databricks-Review"
  },
  "policy": {
    "timeout": "1:00:00"
  }
}
```

**Why This Matters:**
- `DatabricksNotebook` type tells ADF this step executes a Databricks notebook
- The notebook path points to your actual L01A work from the previous lesson
- Timeout prevents runaway jobs from consuming resources indefinitely



### Step 3: Add Your L01B Analytics Activity

**What You're Building:**
The second step that runs your L01B SparkSQL analytics, but only after L01A completes successfully.

**UI Steps:**
1. Drag another **Databricks Notebook** activity onto the canvas
2. Configure in the **General** tab:
   - **Name**: `ExecuteL01B_AdvancedAnalytics`
   - **Description**: `Run L01B fraud analytics with advanced SparkSQL`
   - **Timeout**: `1:30:00` (90 minutes - analytics may take longer)
3. **Connect the activities**: Drag the green arrow from L01A activity to L01B activity
4. In the **Azure Databricks** tab:
   - Select your Databricks linked service
   - **Notebook path**: `/Notebooks/Lesson-01B-SparkSQL-Mastery-Workshop-Solution`

**Behind the Scenes:**
The green arrow connection creates this dependency configuration:
```json
{
  "name": "ExecuteL01B_AdvancedAnalytics",
  "type": "DatabricksNotebook",
  "dependsOn": [
    {
      "activity": "ExecuteL01A_OptimizedProcessing",
      "dependencyConditions": ["Succeeded"]
    }
  ],
  "typeProperties": {
    "notebookPath": "/Notebooks/Lesson-01B-SparkSQL-Mastery-Workshop-Solution"
  },
  "policy": {
    "timeout": "1:30:00"
  }
}
```

**Why This Matters:**
- `dependsOn` ensures L01B only runs after L01A succeeds
- `"Succeeded"` condition means failures in L01A will stop the entire pipeline
- Longer timeout for L01B accounts for complex analytical processing
- The dependency creates proper data lineage and error handling



### Step 4: Add Essential Pipeline Parameters

**What You're Building:**
Basic parameters that make your pipeline reusable with different data sources.

**UI Steps:**
1. Click in empty space on the pipeline canvas
2. In the **Parameters** tab at the bottom, click **+ New**
3. Add these parameters:
   - **Name**: `rawDataPath`, **Type**: String, **Default**: `/banking/raw/transactions/`
   - **Name**: `processedDataPath`, **Type**: String, **Default**: `/banking/processed/fraud_detection/`

**Behind the Scenes:**
Pipeline parameters are stored as:
```json
{
  "parameters": {
    "rawDataPath": {
      "type": "String",
      "defaultValue": "/banking/raw/transactions/"
    },
    "processedDataPath": {
      "type": "String",
      "defaultValue": "/banking/processed/fraud_detection/"
    }
  }
}
```

**Why This Matters:**
- Parameters make your pipeline reusable for different environments (dev, test, prod)
- Default values provide sensible starting points for testing
- These parameters can be passed to your Databricks notebooks for flexibility



## Section 2: Add Production Error Handling (30 minutes)



### Step 5: Configure Essential Retry Logic

**What You're Building:**
Automatic retry capabilities that handle transient failures in your fraud detection pipeline.

**UI Steps:**
1. Select your `ExecuteL01A_OptimizedProcessing` activity
2. In the **General** tab:
   - **Retry**: `2` (retry up to 2 times)
   - **Retry interval**: `300` seconds (5 minutes between retries)
3. Repeat for `ExecuteL01B_AdvancedAnalytics` activity

**Behind the Scenes:**
Retry configuration adds this to your activities:
```json
{
  "policy": {
    "timeout": "1:00:00",
    "retry": 2,
    "retryIntervalInSeconds": 300
  }
}
```

**Why This Matters:**
- Retry logic handles transient failures (network issues, temporary cluster problems)
- 5-minute intervals give time for temporary issues to resolve
- 2 retries balance reliability with execution time



### Step 6: Add Failure Notifications

**What You're Building:**
Automatic notifications when your fraud detection pipeline fails, ensuring your team knows about issues immediately.

**UI Steps:**
1. From **Activities**, drag **Web** activity onto the canvas
2. Name it: `SendFailureAlert`
3. Draw a **red arrow** from your L01A activity to this new activity (failure dependency)
4. Configure the Web activity:
   - **URL**: `https://hooks.slack.com/your-webhook-url` (or your notification endpoint)
   - **Method**: `POST`
   - **Body**: 
   ```json
   {
	"text": "Fraud Detection Pipeline Failed",
	"pipeline": "@pipeline().Pipeline",
	"error": "@activity('ExecuteL01A_OptimizedProcessing').error.message",
	"timestamp": "@utcnow()"
   }
   ```

**Behind the Scenes:**
Failure notifications create this configuration:
```json
{
  "name": "SendFailureAlert",
  "type": "WebActivity",
  "dependsOn": [
    {
      "activity": "ExecuteL01A_OptimizedProcessing",
      "dependencyConditions": ["Failed"]
    }
  ],
  "typeProperties": {
    "url": "https://hooks.slack.com/your-webhook-url",
    "method": "POST",
    "body": {
      "text": "🚨 Fraud Detection Pipeline Failed",
      "pipeline": "@pipeline().Pipeline",
      "error": "@activity('ExecuteL01A_OptimizedProcessing').error.message",
      "timestamp": "@utcnow()"
    }
  }
}
```

**Why This Matters:**
- `"Failed"` dependency condition triggers only when L01A fails
- Pipeline expressions provide context for troubleshooting
- Immediate notifications reduce time to detect and resolve issues
- Webhook integration works with Slack, Teams, or custom alerting systems



### Step 7: Add Basic Data Quality Monitoring

**What You're Building:**
Simple monitoring that checks if your fraud detection pipeline is producing reasonable results.

**UI Steps:**
1. Add a **Lookup** activity after your L01B activity
	- Connect to the Lookup activity from your L01B activity via a green line
2. Name it: `CheckBasicQuality`
3. In the **Settings** tab:
    - **Source dataset**: Click **New** to create a dataset
    - **Dataset type**: Choose "Parquet" (since your outputs are parquet files)
    - **Location**: Point to `/mnt/coursedata/fraud_enriched_transactions`
    - **First row only**: Keep checked ✓
4. Drag an **If Condition** activity from the **Iterations & conditionals** tab, place it after the lookup, and connect to it via a green line
5. Configure the condition:
   - **Expression**: `@greater(activity('CheckBasicQuality').output.firstRow.record_count, 0)`
   - **If False**: Add another Web activity for quality alerts
	   - Click the edit icon for the False case and configure the Web activity as follows:
		   - **Name**: `SendQualityAlert`
		   - **URL**: `https://hooks.slack.com/your-webhook-url`
		   - **Method**: POST
		   - **Body**:
```
{
	"text": "Fraud Detection Quality Check Failed",
	"pipeline": "@{pipeline().Pipeline}",
	"message": "No records found in fraud detection results for run @{pipeline().RunId}",
	"timestamp": "@{utcnow()}",
	"action": "Please check L01A and L01B notebook execution logs"
}
```



Your configuration should look similar to this.

![ADF Pipeline Config](.\assets\L01C-adf-pipeline-config.png)



**Behind the Scenes:**
Quality monitoring creates:

```json
{
  "name": "CheckBasicQuality",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "DeltaSource",
      "query": "SELECT COUNT(*) as record_count FROM processed_fraud_data WHERE process_date = CURRENT_DATE"
    }
  }
},
{
  "name": "QualityCheck",
  "type": "IfCondition",
  "typeProperties": {
    "expression": {
      "value": "@greater(activity('CheckBasicQuality').output.firstRow.record_count, 0)",
      "type": "Expression"
    },
    "ifFalseActivities": [
      {
        "name": "SendQualityAlert",
        "type": "WebActivity"
      }
    ]
  }
}
```

**Why This Matters:**
- Lookup activities can query your processed data for basic health checks
- If Condition activities create smart branching based on data conditions
- Quality alerts catch data processing issues early
- Simple record count checks verify pipeline is producing output



## Section 3: Hands-On Exercises - Build Your Complete Platform (45 minutes)

### Exercise 1: Build Your Integrated Pipeline (15 minutes)

**Your Mission:**
Create a working ADF pipeline that orchestrates your L01A and L01B fraud detection notebooks in sequence, with proper dependencies and parameterization.

#### Task 1A: Create the Pipeline Foundation (5 minutes)

**Detailed Steps:**

1. **Access Azure Data Factory Studio**
   - Navigate to your Azure portal → Find your Data Factory resource
   - Click **"Open Azure Data Factory Studio"** (blue button)
   - Wait for the Data Factory Studio to load completely

2. **Create New Pipeline**
   - In the left navigation, click the **Author** icon (pencil/paper icon)
   - In the **Factory Resources** panel, right-click **Pipelines** 
   - Select **"New pipeline"** from the context menu
   - **Important**: You should see a new pipeline tab open with a blank canvas

3. **Configure Pipeline Properties**
   - Click on empty space in the pipeline canvas (not on any activity)
   - In the **Properties** panel on the right:
     - **Name**: `fraud-detection-exercise-pipeline`
     - **Description**: `Student exercise - Complete fraud detection platform with L01A and L01B integration`
   - **Verify**: The pipeline name appears in the tab at the top

#### Task 1B: Add L01A Processing Activity (4 minutes)

**Detailed Steps:**

1. **Add Databricks Notebook Activity**
   - In the **Activities** panel (left side), expand **Databricks** section
   - **Drag** the **Databricks Notebook** activity onto the canvas
   - **Verify**: You see a blue rectangular activity on the canvas

2. **Configure Activity Properties**
   - **Click once** on the Databricks Notebook activity to select it
   - In the **General** tab at the bottom:
     - **Name**: `ExecuteL01A_OptimizedProcessing` (no spaces allowed)
     - **Description**: `Run L01A fraud detection with optimized PySpark`
     - **Timeout**: Change to `1:00:00` (1 hour)

3. **Configure Databricks Connection**
   - Click the **Azure Databricks** tab at the bottom
   - **Databricks linked service**: Select your existing linked service from dropdown
     - If no linked service exists, click **New** and configure connection to your Databricks workspace
   - **Notebook path**: `/Notebooks/Lesson-01A-Azure-Databricks-Review`
   - **Cluster**: Use existing interactive cluster (or configure job cluster)

4. **Verify Configuration**
   - **Validate** button in top toolbar - should show green checkmark
   - Activity should display proper name on canvas

**Troubleshooting:**
- If linked service fails: Ensure your Databricks workspace is running and accessible
- If notebook path errors: Verify the exact path in your Databricks workspace
- If validation fails: Check all required fields are populated

#### Task 1C: Add L01B Analytics Activity with Dependencies (4 minutes)

**Detailed Steps:**

1. **Add Second Databricks Activity**
   - From **Activities** panel, drag another **Databricks Notebook** to the canvas
   - Position it to the **right** of your L01A activity (leave space for connection)

2. **Configure L01B Activity**
   - Select the new activity, configure in **General** tab:
     - **Name**: `ExecuteL01B_AdvancedAnalytics`
     - **Description**: `Run L01B fraud analytics with advanced SparkSQL`
     - **Timeout**: `1:30:00` (90 minutes for complex analytics)

3. **Configure Databricks Settings**
   - **Azure Databricks** tab:
     - **Databricks linked service**: Same as L01A activity
     - **Notebook path**: `/Notebooks/Lesson-01B-SparkSQL-Mastery-Workshop-Solution`

4. **Create Success Dependency**
   - **Hover** over the L01A activity until you see small boxes appear on the edges
   - **Click and drag** from the green box (success output) to the L01B activity
   - **Verify**: You see a green arrow connecting L01A → L01B
   - **Important**: This ensures L01B only runs if L01A succeeds

**Troubleshooting:**
- If dependency line doesn't appear: Ensure you're dragging from the green success box
- If notebook path is wrong: Double-check the exact path in your Databricks workspace
- If activities overlap: Drag them to better positions for clarity

#### Task 1D: Add Essential Parameters (2 minutes)

**Detailed Steps:**

1. **Access Pipeline Parameters**
   - Click on **empty space** in the pipeline canvas (deselect all activities)
   - At the bottom, click the **Parameters** tab
   - You should see an empty parameters table

2. **Add Data Path Parameters**
   - Click **+ New** button
   - **First Parameter:**
     - **Name**: `rawDataPath`
     - **Type**: String (default)
     - **Default value**: `/banking/raw/transactions/`
   - Click **+ New** again
   - **Second Parameter:**
     - **Name**: `processedDataPath`
     - **Type**: String
     - **Default value**: `/banking/processed/fraud_detection/`

3. **Verify Parameter Configuration**
   - **Save All** in top toolbar
   - Parameters should appear in the table
   - **Note**: These parameters can later be passed to your Databricks notebooks

**Success Verification Checklist:**
- [ ] Two Databricks activities on canvas with descriptive names
- [ ] Green arrow connecting L01A → L01B (success dependency)
- [ ] Both activities configured with correct notebook paths
- [ ] Pipeline parameters defined with sensible defaults
- [ ] Validation shows no errors (green checkmark)
- [ ] Pipeline saves successfully

---

### Exercise 2: Add Essential Error Handling (15 minutes)

**Your Mission:**
Transform your basic pipeline into a production-ready system with retry logic, failure notifications, and proper error handling.

#### Task 2A: Configure Retry Policies (5 minutes)

**Detailed Steps:**

1. **Configure L01A Retry Settings**
   - **Click** on the `ExecuteL01A_OptimizedProcessing` activity
   - In the **General** tab, find the **Advanced** section:
     - **Retry**: Change from `0` to `2`
     - **Retry interval (sec)**: Set to `300` (5 minutes)
   - **Why**: Handles transient failures like network timeouts or temporary cluster issues

2. **Configure L01B Retry Settings**
   - **Click** on the `ExecuteL01B_AdvancedAnalytics` activity
   - **General** tab → **Advanced** section:
     - **Retry**: Set to `2`
     - **Retry interval (sec)**: Set to `300`
   - **Note**: Analytics workloads may be more prone to memory issues, so retries help

3. **Test Retry Configuration**
   - **Save All** changes
   - **Debug** the pipeline (Debug button in top toolbar)
   - Monitor the **Output** tab for retry behavior if any failures occur

**Troubleshooting:**
- If retry settings don't save: Ensure you're in the correct tab and clicking Save All
- If Debug fails immediately: Check your Databricks cluster status and linked service

#### Task 2B: Add Failure Notification System (7 minutes)

**Detailed Steps:**

1. **Add Web Activity for Notifications**
   - From **Activities** panel, expand **General** section
   - **Drag** a **Web** activity onto the canvas
   - Position it **below** your L01A activity

2. **Configure Web Activity Properties**
   - Select the Web activity, **General** tab:
     - **Name**: `SendFailureAlert`
     - **Description**: `Send notification when L01A fraud detection fails`
     - **Timeout**: `0:10:00` (10 minutes)

3. **Create Failure Dependency**
   - **Hover** over the L01A activity to see connection boxes
   - **Click and drag** from the **red box** (failure output) to the Web activity
   - **Verify**: Red arrow connects L01A to notification activity
   - **Important**: This triggers only when L01A fails

4. **Configure Notification Settings**
   - Select Web activity, click **Settings** tab:
   - **URL**: Use a webhook URL for your notification system:
```
https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK
```
     *Or use a Teams webhook, email service, or test URL like httpbin.org/post*
   
   - **Method**: Select `POST` from dropdown
   - **Body**: Click in the body field and enter:
```json
{
	"text": "🚨 Fraud Detection Pipeline Failed",
	"pipeline": "@{pipeline().Pipeline}",
	"runId": "@{pipeline().RunId}",
	"error": "@{activity('ExecuteL01A_OptimizedProcessing').error.message}",
	"timestamp": "@{utcnow()}",
	"action": "Check Databricks logs and retry if necessary"
}
```

5. **Add Headers (if needed)**
   - Click **+ New** under Headers
   - **Name**: `Content-Type`
   - **Value**: `application/json`

**Testing Your Notification:**
- Temporarily change the L01A notebook path to something invalid (like `/invalid/path`)
- **Debug** run the pipeline
- **Verify**: Pipeline fails and sends notification
- **Fix**: Restore correct notebook path

#### Task 2C: Add Failure Notification for L01B (3 minutes)

**Detailed Steps:**

1. **Duplicate Notification Setup**
   - **Right-click** on the `SendFailureAlert` activity
   - Select **Clone** (this copies all settings)
   - **Drag** the cloned activity to position below L01B

2. **Configure L01B Failure Notification**
   - Select the cloned Web activity, **General** tab:
     - **Name**: `SendL01BFailureAlert`
     - **Description**: `Send notification when L01B analytics fails`

3. **Create L01B Failure Dependency**
   - **Delete** the existing connection from the cloned activity
   - **Connect** the red failure box from L01B activity to this notification activity

4. **Update Notification Message**
   - **Settings** tab, update the **Body**:
```json
{
	"text": "🚨 Fraud Analytics (L01B) Failed",
	"pipeline": "@{pipeline().Pipeline}",
	"runId": "@{pipeline().RunId}",
	"error": "@{activity('ExecuteL01B_AdvancedAnalytics').error.message}",
	"timestamp": "@{utcnow()}",
	"action": "L01A succeeded but L01B analytics failed - check SparkSQL queries"
}
```

**Success Verification Checklist:**
- [ ] Both Databricks activities have retry count = 2, interval = 300 seconds
- [ ] Red failure arrows connect each Databricks activity to a Web notification activity
- [ ] Notification activities have valid webhook URLs and proper JSON bodies
- [ ] Pipeline expressions (@ symbols) are correctly formatted
- [ ] Debug run triggers appropriate notifications on failure

---

### Exercise 3: Add Data Quality Monitoring and L03 Preparation (15 minutes)

**Your Mission:**
Add essential data quality checks and prepare your integrated platform for automated deployment in tomorrow's L03 lesson.

#### Task 3A: Implement Data Quality Monitoring (7 minutes)

**Detailed Steps:**

1. **Add Lookup Activity for Quality Check**
   - From **Activities** panel, expand **General** section
   - **Drag** a **Lookup** activity onto the canvas
   - Position it **after** your L01B activity (to the right)

2. **Configure Lookup Activity**
   - Select Lookup activity, **General** tab:
     - **Name**: `CheckBasicQuality`
     - **Description**: `Verify fraud detection results contain data`
     - **Timeout**: `0:05:00` (5 minutes)

3. **Create Success Dependency from L01B**
   - **Connect** green success arrow from L01B to the Lookup activity
   - **Verify**: Lookup only runs after L01B succeeds

4. **Configure Data Source for Quality Check**
   - Click **Settings** tab on Lookup activity
   - **Source dataset**: Click **New** to create a dataset
   - **Dataset Configuration:**
     - **Type**: Choose **Azure Blob Storage Gen2** (or your storage type)
     - **Format**: Select **Parquet** (matches your L01B output)
     - **Linked Service**: Select your storage linked service
     - **File path**: Point to your fraud detection output location (choose the folder that matches your L01B output):
```
Container: [your-container-name]
Directory: fraud_enriched_transactions/
File: (leave blank for folder query)
```
       *Alternative: Use `fraud_detection_results/` if that's where your L01B output is stored*
   
   - **Important**: Since we're using a file-based dataset, we'll get the first row only by default, which is perfect for checking if data exists
   - **First row only**: Leave this **unchecked** if you want to get all records (for more detailed quality checks)
   - **File path type**: Select **File path in dataset** (default)
     
   **Note**: For basic quality monitoring, this Lookup will return the first row/record from your fraud detection results, which we can then check in the If Condition to verify data was produced.

5. **Add Quality Check Logic (Simplified Approach)**
   - **Connect** green success arrow from L01B to the Lookup activity
   - **Verify**: Lookup only runs after L01B succeeds

6. **Add Quality Success/Failure Paths**
   - From **Activities**, drag a **Web** activity onto the canvas (for quality failure alert)
   - Position it below the Lookup activity
   - **Connect** the **red failure arrow** from Lookup to this Web activity
   - **Connect** the **green success arrow** from Lookup to wherever your pipeline should continue (or leave it as the end)

7. **Configure Quality Failure Alert**
   - Select the Web activity connected to Lookup failure, configure:
     - **Name**: `SendQualityAlert`
     - **URL**: Same webhook as other notifications
     - **Method**: POST
     - **Body**:
```json
{
	"text": "⚠️ Data Quality Issue - No Fraud Detection Results Found",
	"pipeline": "@{pipeline().Pipeline}",
	"issue": "Lookup activity failed - no data files found in fraud detection output",
	"timestamp": "@{utcnow()}",
	"action": "Check L01A and L01B execution - verify data is being written to storage"
}
```


#### Task 3B: Document Pipeline Performance (5 minutes)

**Detailed Steps:**

1. **Run Complete End-to-End Test**
   - **Debug** your complete pipeline with real data
   - **Monitor** the **Output** tab for execution details
   - **Record** timing for each activity:

2. **Create Performance Documentation**
   - Create a text file or notebook with these metrics:
```
     FRAUD DETECTION PIPELINE PERFORMANCE METRICS
     ============================================
     
     Pipeline: fraud-detection-exercise-pipeline
     Test Date: [Today's Date]
     Data Volume: [Number of transactions processed]
     
     Activity Performance:
     - ExecuteL01A_OptimizedProcessing: _____ minutes
     - ExecuteL01B_AdvancedAnalytics: _____ minutes  
     - CheckBasicQuality: _____ seconds
     - Total Pipeline Duration: _____ minutes
     
     Resource Usage:
     - Databricks Cluster Size: [e.g., 2-8 workers]
     - Peak Memory Usage: [if available]
     - Estimated Cost: $_____ per run
     
     Quality Metrics:
     - Records Processed: _____
     - High Risk Transactions Detected: _____
     - Data Quality Check Result: PASS/FAIL
     
     Issues/Optimizations Identified:
     - [Any performance bottlenecks]
     - [Suggestions for improvement]
```

3. **Identify Optimization Opportunities**
   - Note which activity takes the longest
   - Consider if cluster size needs adjustment
   - Document any timeout or retry issues

#### Task 3C: Prepare for L03 CI/CD Automation (3 minutes)

**Detailed Steps:**

1. **Export ARM Template**
   - In ADF Studio, click **Manage** tab (cog icon) in left navigation
   - Select **ARM template** from the menu
   - Click **Export ARM template**
   - **Select Resources**: Check your `fraud-detection-exercise-pipeline` and related linked services
   - **Download**: Save the template files to your computer
   - **Verify**: You have both template.json and parameters.json files

2. **Document Deployment Dependencies**
   - Create a deployment checklist:
```
     L03 CI/CD DEPLOYMENT REQUIREMENTS
     ================================
     
     Required Linked Services:
     - [ ] Azure Databricks (configured with workspace URL and access token)
     - [ ] Azure Data Lake Storage Gen2 (with proper permissions)
     - [ ] Key Vault (for storing secrets) - if used
     
     Required Parameters:
     - [ ] rawDataPath: /banking/raw/transactions/
     - [ ] processedDataPath: /banking/processed/fraud_detection/
     - [ ] notificationWebhookUrl: [your webhook URL]
     
     Manual Configuration Steps:
     - [ ] Upload L01A and L01B notebooks to Databricks workspace
     - [ ] Configure Databricks cluster with required libraries
     - [ ] Set up storage permissions for Data Factory managed identity
     - [ ] Test notification webhooks
     
     Environment-Specific Settings:
     - DEV: [development values]
     - TEST: [testing values]  
     - PROD: [production values]
```

3. **Validate Complete Platform**
   - **Final end-to-end test**: Run entire pipeline and verify:
     - [ ] L01A executes successfully
     - [ ] L01B runs after L01A completes
     - [ ] Data quality check validates results
     - [ ] Failure notifications work (test with invalid paths)
     - [ ] All retry logic functions properly
   - **Save and Publish**: Use **Publish** button to save to ADF service

**Success Verification Checklist:**
- [ ] Lookup activity successfully connects to your fraud detection output folder
- [ ] Quality failure alert (Web activity) connected to red failure arrow from Lookup
- [ ] Quality alerts provide actionable troubleshooting information  
- [ ] Pipeline performance metrics documented with specific timings
- [ ] ARM template exported successfully with all dependencies
- [ ] Deployment checklist created for L03 automation
- [ ] Complete end-to-end test passes with real data
- [ ] Data quality monitoring triggers appropriate alerts when no output files exist

**Final Platform Status:**
🎯 **Congratulations!** You now have a complete enterprise-grade fraud detection platform that:
- ✅ Orchestrates L01A PySpark optimization with L01B SparkSQL analytics
- ✅ Handles failures gracefully with retries and notifications
- ✅ Monitors data quality and alerts on issues
- ✅ Ready for automated deployment in tomorrow's L03 lesson

---

## Common Issues and Quick Fixes

### Pipeline Execution Issues
- **"Notebook not found"**: Verify exact notebook paths in Databricks workspace
- **"Linked service authentication failed"**: Check Databricks workspace access and permissions
- **"Timeout exceeded"**: Increase timeout values or optimize cluster configuration

### Notification Issues  
- **Webhooks not working**: Test URL independently with curl or Postman
- **Expression errors**: Verify @ symbol syntax and proper escaping in JSON
- **Missing notifications**: Check red failure arrow connections are properly configured

### Data Quality Issues
- **Lookup activity fails**: Check that fraud detection output folder exists and contains files
- **No quality alerts when expected**: Verify red failure arrow connection from Lookup to Web activity
- **Permission errors**: Ensure Data Factory managed identity has Storage Blob Data Reader permissions
- **Wrong folder path**: Confirm the dataset points to where L01B actually writes output

### Deployment Preparation
- **ARM export fails**: Ensure all resources are in same resource group and subscription
- **Missing dependencies**: Include all linked services in ARM template export
- **Parameter mismatches**: Verify parameter names match between template and pipeline


## Quick Troubleshooting Guide

### Common Issues and Fast Fixes

**Problem**: Pipeline fails to run
**Quick Fix**: Check linked service connections and notebook paths

**Problem**: L01B doesn't wait for L01A
**Quick Fix**: Verify green arrow connection and dependency conditions

**Problem**: Notifications not working
**Quick Fix**: Test webhook URL independently and check network connectivity

**Problem**: ARM template export fails
**Quick Fix**: Ensure all resources are in the same resource group



## Conclusion and Next Steps

**What You've Accomplished in 120 Minutes:**

You've successfully built a complete enterprise-grade fraud detection platform that:

- ✅ **Orchestrates L01A → L01B** with proper dependencies and error handling
- ✅ **Handles failures gracefully** with retry logic and automatic notifications
- ✅ **Monitors data quality** with basic health checks and alerting
- ✅ **Ready for L03 automation** with ARM templates and deployment documentation

**Your Platform Journey:**
- ✅ **L01A**: Optimized PySpark fraud detection with production-ready error handling
- ✅ **L01B**: Advanced SparkSQL analytics with sophisticated fraud pattern detection
- ✅ **L01C**: Integrated ADF orchestration platform with monitoring and error handling
- 🎯 **Tomorrow (L03)**: Automated deployment and CI/CD for your complete platform

**Key Skills Demonstrated:**
- **Pipeline Orchestration**: Successfully integrating multiple data processing components
- **Production Error Handling**: Implementing retry logic and failure notifications
- **Data Quality Monitoring**: Adding basic health checks and alerting
- **Deployment Preparation**: Creating ARM templates and documentation for automation

**Ready for L03 CI/CD:**
Your fraud detection platform is now fully prepared for automated deployment:
- ✅ ARM templates exported and documented
- ✅ All dependencies and parameters identified
- ✅ Error handling and monitoring tested and working
- ✅ Performance benchmarks established

Tomorrow's L03 lesson will take your platform and automate its deployment using CI/CD practices, completing your journey from individual components to enterprise-automated data platform.



## Optional Advanced Topics

*For students who finish early or want to explore further:*

### Bonus Module A: Advanced Performance Optimization (30 minutes)
- Parallel processing with ForEach activities
- Dynamic resource allocation based on data volume
- Schedule-based cluster optimization

### Bonus Module B: Enterprise Monitoring (30 minutes)
- Complex business metrics collection
- Custom alerting rules and thresholds
- Integration with Azure Monitor and Log Analytics

These bonus modules are available as self-study materials or can be covered in extended workshop sessions.