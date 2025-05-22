
---

## 🛠️ End-to-End Process

### 1. ✅ SQL Server Setup (On-Premise)
- Installed **SQL Server 2022 Express** & **SSMS** locally.
- Restored `AdventureWorksLT2022.bak` database from Microsoft.

### 2. ✅ Azure Setup
Created a **Resource Group** and the following services:
- **Data Factory**: For orchestration
- **Data Lake Storage**: For Bronze → Silver → Gold data
- **Databricks**: For transformations using PySpark
- **Synapse Analytics**: For procedures + integration
- **Power BI**: For data visualization

---

## 🔄 Data Pipeline Stages

### 🔹 ADF: Bronze Layer

- Created `Copy Data` pipeline to load data from on-prem SQL Server to **Bronze** container in Data Lake.
- Used **Linked Service** for SQL Server & Data Lake connection.
- In `ForEach` activity:
  - **Lookup Query**:
    ```sql
    SELECT s.name AS SchemaName, t.Name AS TableName
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'SalesLT'
    ```
  - **Query for Each Table**:
    ```json
    @{concat('SELECT * FROM ', item().SchemaName, '.', item().TableName)}
    ```

> 📤 Output: Raw data in `.parquet` format in Bronze folder.

---

### 🔸 Databricks: Silver & Gold Layers

- **Mounted the Data Lake** inside Databricks
- Transformed data:
  - Cleaned nulls
  - Standardized columns
  - Filtered data
- **bronze to silver-extended.ipynb**: Bronze → Silver  
- **silver to Gold transformation.ipynb**: Silver → Gold

> 🧪 Cluster Type: Single Node  
> 📦 Output Format: `.parquet` files in Silver & Gold folders

---

### 🔹 Synapse Analytics: Stored Procedure Integration

- Used `GetMetadata` and `Stored Procedure` activities
- Created and triggered stored procedures to load structured data

---

### 📈 Power BI: Sales KPI Dashboard

- Connected to Azure Data Lake using **Power BI Desktop**
- Visualized:
  - Total Sales
  - Products Sold
  - Gender Split
  - Category-wise breakdown
- Slicers: Gender, Product Category, Date Range

---

## 🚧 Improvements (To Be Made)

- Add visual filters to switch between raw → processed data
- Display null/missing value stats in Power BI
- Add interactive date filters & annotations
- Upload .csv exports for downloadable insights

---

## 💡 Key Learnings

- Hands-on with Data Lake and ADF pipelines
- Managed layered architecture (Bronze → Silver → Gold)
- Understood real-time integration between ADF and Databricks
- Applied parameterized pipeline design
- Data transformation using PySpark
- Deployment-ready visualization with Power BI

---

## 📸 Screenshots

> 📌 ADF Pipeline Flow  
![ADF Pipeline](./)

> 📌 ForEach Transformation Setup  
![ForEach](./)

---

## 🧠 Author

**Rohith Chaduvala**  
💼 Data Engineering Enthusiast | Azure | Python | PySpark    
🔗 GitHub: [RohithChaduvala](https://github.com/RohithChaduvala)

---

> ⭐ *Star this repo if it helped you or gave you project inspiration!*
