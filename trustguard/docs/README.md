# TrustGuard — Data Quality Pipeline

> A production-grade, end-to-end data quality pipeline built on PySpark + Delta Lake.  
> Ingests dirty retail transaction data, validates, cleans, analyzes, and flags anomalies.

---

## Architecture

```text
Source CSV
    │
    ▼
┌─────────────────────────────────────────┐
│          Module 1: Ingestion            │
│  Schema check → Raw Delta tables        │
│  data/raw/transactions_raw              │
│  data/raw/customers_raw                 │
│  data/raw/ingestion_metadata            │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│        Module 2: DQ Checks              │
│  Completeness / Uniqueness / Format     │
│  Range / Consistency / Referential      │
│  → data/clean/dq_report  (BEFORE)       │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│         Module 3: Cleaning              │
│  Null handling / Dedup / Dates          │
│  Text norm / Type cast / City fix       │
│  → data/clean/transactions_clean        │
│  → data/clean/customers_clean           │
│  → data/clean/rejected_records          │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│       Module 4: SQL Analysis            │
│  final_transactions / customer_summary  │
│  city_sales_report / dq_report          │
│  Monthly revenue / Top customers        │
│  Payment share / Category breakdown     │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│      Module 5: Anomaly Detection        │
│  Qty outliers (>3σ) / Neg prices        │
│  → data/final/anomaly_log               │
│  (non-blocking — final tables untouched)│
└─────────────────────────────────────────┘
                 │
                 ▼
         pipeline_log (audit trail per run)
```

---

## Project Structure

```text
TrustGuard/
├── TrustGuard_Problem_Statement 2.docx
└── trustguard/
    ├── data/
    │   ├── raw/            # Ingested Delta tables & metadata (from source CSV)
    │   ├── clean/          # Cleaned Delta tables + rejected_records
    │   └── final/          # Analysis-ready Delta tables + pipeline_log
    ├── notebooks/          # Python pipeline modules (PySpark)
    │   ├── 01_ingestion.py
    │   ├── 02_dq_checks.py
    │   ├── 03_cleaning.py
    │   ├── 04_sql_analysis.py
    │   ├── 05_anomaly_detection.py
    │   └── run_pipeline.py
    ├── lookups/
    │   └── city_corrections.csv
    ├── sql/                # Reusable SQL queries
    │   ├── monthly_revenue_by_city.sql
    │   ├── top_10_customers.sql
    │   ├── payment_method_share.sql
    │   └── category_breakdown.sql
    ├── hadoop/             # Windows PySpark dependencies (winutils.exe, hadoop.dll)
    │   └── bin/
    └── docs/
        └── README.md       ← you are here
```

---

## Dataset

**Retail Store Sales – Dirty Data for Cleaning**  
Source: [Kaggle](https://www.kaggle.com/datasets/ahmedmohamedali/retail-store-sales-dirty-for-data-cleaning) | ~10,000 rows  
Columns: `Transaction_ID`, `Customer_ID`, `Category`, `Item`, `Price_per_Unit`, `Quantity`, `Total_Spent`, `Payment_Method`, `Location`, `Date`

Known data issues (documented for DQ baseline):
- ~8% null `Category` values
- ~2.5% duplicate `Transaction_ID`s
- Mixed date formats: `YYYY-MM-DD`, `DD-MM-YYYY`, `MM/DD/YY`
- City name typos: `Mumabi`, `Bangalor`, etc.
- Inconsistent `Payment_Method` casing

---

## How to Run

### Local (PySpark on Windows)

```bash
pip install pyspark delta-spark pandas

# The pipeline requires the source CSV to be placed in the `data/raw/source/` folder.
# Create the directory if it doesn't exist and add `retail_store_sales.csv`:
mkdir -p ../data/raw/source
# (Place retail_store_sales.csv inside ../data/raw/source/)

# Note: The included `hadoop/bin/` folder is automatically configured by the scripts 
# to support local execution on Windows without additional environment setup.

cd trustguard/notebooks
python run_pipeline.py retail_store_sales.csv
```

---

## Before / After DQ Metrics

| Check | Before | After | Target |
|---|---|---|---|
| Null `Category` | ~8% | 0% | 0% |
| Duplicate `Transaction_ID` | ~2.5% | 0% | 0% |
| Non-standard dates | ~15% | 0% | 0% |
| Revenue accuracy | baseline | ±0.5% | ±0.5% |
| Referential integrity | ~1% orphans | 0% | 0% |

*Actual numbers will populate in `data/clean/dq_report` after your first full pipeline run.*

---

## Pipeline Log (Audit Trail)

Each run appends to `data/final/pipeline_log` with:

| Column | Description |
|---|---|
| `run_id` | UUID per pipeline run |
| `run_timestamp` | UTC timestamp |
| `module` | Which module ran |
| `layer` | raw / clean / final |
| `records_in` | Input row count |
| `records_out` | Output row count |
| `records_rejected` | Rows sent to rejected_records |
| `status` | SUCCESS / FAILED |
| `error_message` | Error detail on failure |

---

## Key Design Decisions

- **All-STRING raw layer**: avoids type-inference crashes; every cast happens explicitly in Module 3
- **Nothing silently dropped**: every rejected row has a `reason` column in `rejected_records`
- **Reconciliation check**: `raw_count == clean_count + rejected_count` verified after every run
- **Anomaly detection is non-blocking**: flagged rows go to `anomaly_log`, final tables are untouched
- **City corrections via lookup**: reusable reference-table pattern, easy to extend

---

## Future Work

- [ ] Schedule via Airflow
- [ ] Swap rule-based anomaly detection for Isolation Forest (scikit-learn / MLflow)
- [ ] Power BI SQL dashboard on `dq_report` and `city_sales_report`
- [ ] Email/Teams alert when DQ failure rate crosses threshold
- [ ] Scale test on 500K / 1M-row dataset
- [ ] Formal unit tests (pytest + chispa for PySpark)
