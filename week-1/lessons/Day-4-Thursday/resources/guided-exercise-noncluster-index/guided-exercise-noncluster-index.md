# Guided Exercise: Nonclustered Indexing in InsuranceDB

## Objective

In this asynchronous guided exercise, you’ll recreate the nonclustered indexing demonstration from May 22, 2025, using the `InsuranceDB` database. You’ll build a `Policies` table, import 1000 policies from `policies_data.csv`, create a nonclustered index on `PolicyNumber`, verify it, and compare query performance with and without the index. This hands-on activity reinforces the lesson’s concepts (how indexes work, verification, performance benefits) and prepares you for the `RetailSales` assessment’s indexing task.

## Setup

- **Tools**: Azure Data Studio, connected to your SQL Server instance.
- **Files**:
  - `policies_data.csv` (1000 rows, provided by the instructor).
  - This SQL script (`insurance_indexing_exercise.sql`).
- **Prerequisites**:
  - Read the “Understanding Nonclustered Indexing in SQL Server” lesson (20–30 minutes).
  - Basic SQL knowledge (`SELECT`, `INSERT`, `CREATE TABLE`).
  - Access to `policies_data.csv`.
- **Duration**: 20–30 minutes.
- **Instructions**:
  1. Save `policies_data.csv` to your computer.
  2. Copy this script into a new query window in Azure Data Studio.
  3. Execute each step sequentially, checking outputs in the Results or Messages tab.
  4. Save your script with outputs (e.g., copy Results pane text) as `yourname_indexing_exercise.sql` for submission, if required.

### Guided Exercise Script

See the accompanying SQL file for step-by-step instructions to implement and test your nonclustered index.

## Guided Exercise Instructions

1. **Read the Lesson**: Complete the “Understanding Nonclustered Indexing in SQL Server” lesson (20–30 minutes) to understand nonclustered indexes.
   
2. **Set Up**:
   - Open Azure Data Studio, connect to your SQL Server instance, and create a new query window.
   - Download `policies_data.csv` (from your instructor) and note its file path.
   - Copy this SQL script (`insurance_indexing_exercise.sql`) into the query window.
     
1. **Execute Steps**:
   - Run each step (1–11) sequentially, checking outputs in the Results or Messages tab.
   - For Step 4, follow the Import Wizard instructions to load `policies_data.csv`. If stuck, consult Azure Data Studio’s help or ask in the course forum.
   - Verify outputs match the expected results:
     - Step 4: `RowCount = 1000`.
     - Step 5: 5 rows with policy details.
     - Step 6/9/11: 1 row (if `PolicyNumber=2296493796` exists; random, so may vary).
     - Step 8: `index_name=IX_Policies_PolicyNumber`, `table_name=Policies`, `column_name=PolicyNumber`.
     - Step 10: No rows.
   - Note `elapsed time` in Messages for Steps 6, 9, 11 to compare performance (differences may be small with 1000 rows).
   - Step 12 is optional; skip if not submitting the database.
     
1. **Reflect**:
   - Compare Steps 6, 9, and 11’s `elapsed time` in Messages. With 1000 rows, differences are small, but the demo showed 1–2 ms with the index vs. 20–50 ms without for larger data.
   - Step 8 confirms the index, like checking a library catalog.
   - How does this relate to the lesson’s analogies (e.g., book index)?

### Expected Outputs

- **Step 4**: `RowCount = 1000`
- **Step 5**: 5 rows with `PolicyID`, `PolicyNumber`, `EffectiveDate`, `PremiumAmount` (e.g., `PolicyNumber=2296493796`, `PremiumAmount=1200.00`, if present).
- **Step 6/9/11**: 1 row (if `PolicyNumber=2296493796` exists; randomly generated, so may vary). Messages show `elapsed time` (e.g., 1–2 ms).
- **Step 8**:
  ```
  index_name                table_name  column_name
  IX_Policies_PolicyNumber  Policies    PolicyNumber
  ```
- **Step 10**: No rows (index removed).

