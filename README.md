# Netflix-Azure-Data-Engineering-Project
Netflix-Azure-Data-Engineering-Project


## Description
This project demonstrates a modern data engineering pipeline for processing the Netflix dataset using Azure services. The project leverages Azure Data Factory (ADF), Azure Data Lake Storage Gen2 (ADLS Gen2), Azure Databricks, and Delta Live Tables (DLT) to ingest, transform, validate, and store data in Bronze, Silver, and Gold layers following the Medallion Architecture. The curated data can be consumed for advanced analytics, reporting, and data quality monitoring. 
Data is ingested from a GitHub repository into Azure Data Lake Storage Gen2 (ADLS), processed and transformed using Azure Data Factory (ADF) and Azure Databricks, and finally curated and validated using Delta Live Tables (DLT).

The dataset used in this project is Netflix-based metadata files such as:
  netflix_titles.csv
  netflix_directors.csv
  netflix_cast.csv
  netflix_countries.csv
  netflix_category.csv

---

### **Solution Architecture**
  GitHub Repository
        |
        v
Azure Data Factory (ADF)
    - Web Activity
    - Set Variable
    - Validation
    - ForEach + Copy Activity
        |
        v
ADLS Gen2 (Bronze Layer - Raw files)
        |
        v
Databricks Streaming + Auto Loader
        |
        v
ADLS Gen2 (Silver Layer - Cleansed Data)
        |
        v
Delta Live Tables (DLT)
        |
        v
Gold Layer (Business Ready Tables)

---

### **High-Level Pipeline Flow**

#### **1️⃣ Data Ingestion (Azure Data Factory)**
- Data is fetched dynamically from a GitHub repository.
- Ingestion uses the following ADF activities:
  - Web Activity
  - Set Variable
  - Validation
  - ForEach
  - Copy Activity
- Raw CSV files are stored in the **Bronze layer** of **Azure Data Lake Storage Gen2 (ADLS Gen2)** bronze container.

#### **2️⃣ Bronze ➝ Silver Transformation (Azure Databricks)**
- Databricks **Auto Loader (`cloudFiles`)** is used for:
  - Incremental file ingestion
  - Schema inference and evolution
  - Checkpointing for fault-tolerance
- Parameterized **PySpark** notebooks transform data (cleanup, type casting, deduplication).
- Processed DataFrame outputs are stored as **Delta tables** in the **Silver layer**.

#### **3️⃣ Silver ➝ Gold Processing (Delta Live Tables)**
- Applied transformations using **DLT pipelines**.
- Business rules and DQ (data quality) checks applied using decorators such as:
  - `@dlt.table`
  - `@dlt.expect_all_or_drop`
  - `@dlt.expect_all`
- Final curated datasets are published in the **Gold** layer, ready for BI tools.

---

## 🔧 Technologies Used

- **Azure Data Factory**
- **Azure Data Lake Storage Gen2**
- **Azure Databricks**
- **Azure Workflows/pipeline/lakeflow spark declaritive pipeline** 
- **Delta Live Tables**
- **PySpark**
- **GitHub**

---

## Features
- Automated data ingestion from Github public repository via Azure Data Factory (ADF) into **bronze layer**.
- Incremental streaming ingestion from **Bronze layer** using Databricks Auto Loader.
- Parameter-driven data transformation and schema enforcement using Databricks notebooks, saving cleansed data in the **Silver layer**.
- Advanced transformations, windowing, type casting, and business rule validations in the **Silver layer** using databricks.
- Creation of **Gold tables** using **Delta Live Tables (DLT)** with data expectations rules for quality checks.
- Fully orchestrated pipelines using Databricks Workflows for modular execution.
- Data transformation and processing in Databricks, storing the refined data in the **silver layer**.

---

## 🧠 Key Learnings

- Implemented **Medallion Architecture** for scalable data organization.
- Leveraged **Auto Loader** for seamless incremental ingestion.
- Validated data quality using **Delta Live Tables** expectations.
- Produced analytics-ready data for consumption by stakeholders and BI platforms.

---

## 🚀 Step-by-Step Implementation

### 1️⃣ Data Ingestion using Azure Data Factory  
ADF Activities Used: Web Activity, Set Variable, Validation, ForEach Loop, Copy Activity.  
Purpose: Fetch GitHub raw CSVs dynamically, and copy to Bronze layer.  
Files are dynamically written to: `abfss://raw@netflixprojectdlvinay.dfs.core.windows.net/<folder>/<file>.csv`  
ADF uses a parameterized array named `p_array` to iterate over metadata and create folder/file structure.

### 2️⃣ Bronze Layer Processing in Databricks (Notebook: 1_Autoloader)  
Read Bronze data using Auto Loader (cloudFiles):  
`spark.readStream.format("cloudFiles")...load("abfss://raw@netflixprojectdlvinay.dfs.core.windows.net")`  
Write to Bronze layer: stream write with checkpoint and trigger availableNow. Target path example: `abfss://bronze@netflixprojectdlvinay.dfs.core.windows.net/netflix_titles`

### 3️⃣ Silver Layer Transformation (Notebook: 2_silver)  
Use Databricks widgets for parameters: sourcefolder and targetfolder.  
Read CSV from Bronze: `spark.read.format("csv")...load(f"abfss://bronze@.../{var_src_folder}")`  
Write as Delta to Silver: `df.write.format("delta")...save(f"abfss://silver@.../{var_tgt_folder}")`

### 4️⃣ Automation using Lookup Notebook  
Define a JSON array mapping sourcefolder → targetfolder for datasets (directors, cast, countries, category).  
Use `dbutils.jobs.taskValues.set(key="my_array", value=files)` to pass the mapping to workflow.  
Databricks Workflow iterates over this array and executes 2_silver for each mapping.

### 5️⃣ Business Transformation Notebook (4_silver)  
Perform data cleaning and type casting: fill nulls, cast duration fields to integer.  
Split columns: title → Shorttitle, rating → rating.  
Create flags: type_flag based on type (Movie or TV Show).  
Apply window function: `dense_rank` over duration descending.  
Create temporary views: temp_view and global_view.  
Write final cleaned data to Silver layer (Delta) path: `abfss://silver@.../netflix_titles`.

### 6️⃣ Gold Layer using Delta Live Tables (DLT)  
Define data quality rules, e.g. show_id is not null.  
Create DLT tables for directors, cast, countries, category, titles, each using `@dlt.table` and `@dlt.expect_all_or_drop`.  
Load from Silver Delta paths and publish to Gold layer as streaming Delta tables.

### 5️⃣ Business Transformation Notebook (4_silver)  
Perform data cleaning and type casting: fill nulls, cast duration fields to integer.  
Split columns: title → Shorttitle, rating → rating.  
Create flags: type_flag based on type (Movie or TV Show).  
Apply window function: `dense_rank` over duration descending.  
Create temporary views: temp_view and global_view.  
Write final cleaned data to Silver layer (Delta) path: `abfss://silver@.../netflix_titles`.

### 6️⃣ Gold Layer using Delta Live Tables (DLT)  
Define data quality rules, e.g. show_id is not null.  
Create DLT tables for directors, cast, countries, category, titles, each using `@dlt.table` and `@dlt.expect_all_or_drop`.  
Load from Silver Delta paths and publish to Gold layer as streaming Delta tables.

---


## ✅ Final Outcomes

- Automated, parameter-driven ingestion using ADF
- Streaming ingestion using Databricks Auto Loader
- Cleaned, structured Delta tables in Silver layer
- Business-ready, rule-enforced Gold tables via DLT
- Modular pipelines and workflows for scalability
---

## 🏁 Getting Started

### **Prerequisites**
- Azure Subscription with:
  - Data Factory
  - Databricks Workspace
  - Data Lake Storage Gen2
- Git installed locally

### Installation
1. Clone this repository:
   ```bash
    git clone https://github.com/vinaysinghverma07/Netflix-Azure-Data-Engineering-Project.git

### **Why This Structure Works for Your Case**:
1. **Description**: Tailored to your actual architecture using ADF, ADLS Gen2, Databricks, Workflows and DLT.
2. **Features**: Focuses on the Azure & Netflix dataset ingestion and transformation workflow.
3. **Architecture**: Highlights the layers (Medallion architecture (bronze, silver, Gold)) and the tools used.
4. **Usage**: Clear steps for setting up and running the pipeline.


Let me know if you need further modifications!
