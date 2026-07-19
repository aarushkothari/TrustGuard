# TrustGuard: Data Quality Pipeline for Retail Data
**Celebal Excellence Internship - Data Engineering Capstone Project**

**Author:** Aarush Kothari 

# TrustGuard 🛡️

TrustGuard is an end-to-end data engineering pipeline built with **PySpark** and **Delta Lake** designed to process retail store sales data. It automates data ingestion, validates data quality, cleans anomalies, performs SQL-based analytics, and conducts advanced anomaly detection while maintaining a comprehensive pipeline audit log.

## 🚀 Architecture and Modules

The pipeline consists of five core modules that execute sequentially:

1. **`01_ingestion.py` (Data Ingestion)**
   - Reads raw CSV files (e.g., `retail_store_sales.csv`).
   - Validates schemas and standardizes column names.
   - Saves raw transactions and derived customers data into Delta tables.
   - Logs ingestion metadata.

2. **`02_dq_checks.py` (Data Quality Checks)**
   - Assesses the ingested data for missing values, duplicates, and invalid formats.
   - Ensures data integrity before downstream processing.

3. **`03_cleaning.py` (Data Cleaning)**
   - Cleans the raw data by handling nulls, formatting dates, and filtering out invalid records.
   - Saves the cleaned dataset into the `clean` layer in Delta format.

4. **`04_sql_analysis.py` (SQL Analysis)**
   - Executes analytical queries (e.g., finding the top 10 customers by spend).
   - Generates aggregated summary tables for downstream business intelligence.

5. **`05_anomaly_detection.py` (Anomaly Detection)**
   - Identifies outliers and anomalous patterns in the sales data (e.g., unusually high transaction amounts).
   - Flags suspicious records for further review.

## 📁 Repository Structure

```text
TrustGuard/
├── .gitignore
├── README.md                               # Project documentation
├── TrustGuard_Problem_Statement 2.docx     # Original Problem Statement
└── trustguard/
    ├── data/                               # Contains Delta tables and logs
    │   ├── clean/
    │   ├── final/
    │   └── raw/
    ├── docs/                               # Secondary documentation
    ├── hadoop/                             # Local Hadoop binaries for Windows PySpark
    ├── lookups/                            # Lookup tables and reference data
    │   └── city_corrections.csv
    ├── notebooks/                          # PySpark scripts and pipeline runner
    │   ├── 01_ingestion.py
    │   ├── 02_dq_checks.py
    │   ├── 03_cleaning.py
    │   ├── 04_sql_analysis.py
    │   ├── 05_anomaly_detection.py
    │   ├── run_pipeline.py
    │   └── spark-warehouse/
    └── sql/                                # SQL queries for downstream analysis
        ├── category_breakdown.sql
        ├── monthly_revenue_by_city.sql
        ├── payment_method_share.sql
        └── top_10_customers.sql
```

## 🛠️ Setup and Installation

### Prerequisites
- **Python 3.8+**
- **PySpark 3.x**
- **Delta Spark**

### Environment Configuration
The project is configured to run locally (including on Windows). For Windows execution, ensure that the `hadoop/` directory contains `winutils.exe` and `hadoop.dll`, which the script automatically maps to `HADOOP_HOME`.

### Installing Dependencies
```bash
pip install pyspark delta-spark
```

## 🏃‍♂️ Running the Pipeline

You can run the entire pipeline end-to-end using the driver script. The driver executes all modules sequentially and logs the status of each step into a Delta table (`pipeline_log`).

```bash
cd trustguard/notebooks
python run_pipeline.py retail_store_sales.csv
```

### Pipeline Audit Log
The `run_pipeline.py` script maintains a robust audit log in the `data/final/pipeline_log` Delta table. It records:
- `run_id` and `run_timestamp`
- `module` and `layer`
- Records ingested, processed, and rejected
- Execution `status` (SUCCESS/FAILED) and any error messages

## 📊 Sample SQL Analysis
SQL scripts are located in the `sql/` directory. For example, to find top customers:
```sql
SELECT
    Customer_ID,
    ROUND(total_spend, 2) AS total_spend,
    order_count
FROM customer_summary
ORDER BY total_spend DESC
LIMIT 10;
```

## 🛡️ License
This project is open-source and available under the MIT License.
