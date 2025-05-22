# End-to-End Sales Data Pipeline using Azure Data Factory and PySpark

This project demonstrates a full-scale **Data Engineering workflow** using a medallion architecture (Bronze → Silver → Gold) to process sales data sourced from an **on-premises SQL Server** into **Azure Data Lake Storage (ADLS)** using **Azure Data Factory (ADF)** and transformed with **PySpark** in **Databricks**.

---

## 🧱 Architecture: Medallion Layers

### 1. Bronze Layer (Raw Ingestion)
- Data copied using ADF from on-prem SQL Server to **ADLS Gen2** in **Parquet** format.
- Uses `Lookup` + `ForEach` activity in ADF to dynamically loop through all tables in the `SalesLT` schema.
- Query used in Lookup:
  ```sql
  SELECT s.name AS SchemaName, t.Name AS TableName
  FROM sys.tables t
  INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
  WHERE s.name = 'SalesLT'
Dynamic query in ForEach:

python
Copy
Edit
@{concat('select * from ', item().SchemaName, '.', item().TableName)}
2. Silver Layer (Cleansed Data)
Notebook: bronze_to_silver-extended.ipynb

Tasks:

Reads parquet files from Bronze layer.

Applies necessary transformations (e.g., dropping nulls, type conversions).

Stores cleaned output back to ADLS in Silver zone (in Parquet format).

Includes intentional errors to simulate common mistakes and help in debugging.

3. Gold Layer (Business Aggregations)
Notebook: silver_to_gold_transformation.ipynb

Tasks:

Reads refined data from Silver layer.

Performs joins, aggregations, and filtering.

Outputs to the Gold layer for reporting and analytics.

📁 Folder Structure
project-root/
├── bronze_to_silver-extended.ipynb
├── silver_to_gold_transformation.ipynb
├── Sales_dash.pbix (optional - Power BI dashboard, not used currently)
├── README.md

🧪 Common Mistakes & Debugging Tips
Errors in the notebooks are intentional to help you identify:

Issues with Spark session or file path

Data type mismatches

File not found errors

Schema mismatches

Each section is commented to explain what it does, why it might fail, and how to resolve it.

🛠 Technologies Used
Azure Data Factory

Azure Data Lake Storage Gen2

Azure Databricks (PySpark)

On-Premises SQL Server (via Self-Hosted Integration Runtime)

Parquet File Format

 Power BI for visualization

🚀 Future Improvements
Develop monitoring dashboards (e.g., using Azure Monitor or Power BI)

🙋‍♂️ Author
Rohith Chaduvala
GitHub: RohithChaduvala
