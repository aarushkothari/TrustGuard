# =============================================================================
# TrustGuard — Module 2: Data Quality Checks
# =============================================================================
# Goal: Profile raw data and produce a "before" DQ report.
#       All checks run against RAW tables — nothing is modified here.
# =============================================================================

import sys, os, uuid
from datetime import datetime, timezone

# Configure environment variables for local Windows PySpark execution
if os.name == 'nt':
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        current_dir = os.getcwd()
    hadoop_dir = os.path.abspath(os.path.join(current_dir, "..", "hadoop"))
    if os.path.exists(hadoop_dir):
        os.environ["HADOOP_HOME"] = hadoop_dir
        # Add hadoop/bin to PATH so that hadoop.dll can be loaded by the JVM
        hadoop_bin = os.path.join(hadoop_dir, "bin")
        if hadoop_bin not in os.environ["PATH"]:
            os.environ["PATH"] += os.pathsep + hadoop_bin

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, sum as _sum, regexp_extract,
    trim, lower, round as _round, abs as spark_abs
)

builder = SparkSession.builder \
    .appName("TrustGuard-DQChecks") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Detect environment and resolve BASE_PATH
if "dbutils" in globals():
    BASE_PATH = "/Volumes/workspace/default/trustguard_volume"
else:
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        current_dir = os.getcwd()
    BASE_PATH = os.path.abspath(os.path.join(current_dir, ".."))
    builder = builder.config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1")
    # Configure HADOOP_HOME for local Windows environment
    os.environ["HADOOP_HOME"] = os.path.join(BASE_PATH, "hadoop")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
RAW_PATH  = f"{BASE_PATH}/data/raw"
CLEAN_PATH= f"{BASE_PATH}/data/clean"
DQ_PATH   = f"{BASE_PATH}/data/clean/dq_report"

RUN_ID     = str(uuid.uuid4())
CHECKED_AT = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

# ---------------------------------------------------------------------------
# Accumulators for results
# ---------------------------------------------------------------------------
dq_results = []   # list of dicts -> (run_id, dataset, check_name, column, fail_count, fail_rate, checked_at)


def record(dataset, check_name, column, fail_count, total):
    rate = round(fail_count / total, 4) if total > 0 else 0.0
    dq_results.append({
        "run_id":      RUN_ID,
        "dataset":     dataset,
        "check_name":  check_name,
        "column":      column,
        "fail_count":  fail_count,
        "fail_rate":   rate,
        "checked_at":  CHECKED_AT,
    })
    status = "FAIL" if fail_count > 0 else "PASS"
    print(f"  [{status}] {dataset}.{column} | {check_name}: {fail_count:,} failures ({rate:.1%})")


# ---------------------------------------------------------------------------
# Check 1: Completeness — % null/empty per column
# ---------------------------------------------------------------------------
def check_completeness(df, dataset_name):
    print(f"\n--- Completeness ({dataset_name}) ---")
    total = df.count()
    for c in df.columns:
        fail_count = df.filter(col(c).isNull() | (trim(col(c)) == "")).count()
        record(dataset_name, "completeness", c, fail_count, total)


# ---------------------------------------------------------------------------
# Check 2: Uniqueness — duplicate primary keys
# ---------------------------------------------------------------------------
def check_uniqueness(df, dataset_name, pk_col):
    print(f"\n--- Uniqueness ({dataset_name}.{pk_col}) ---")
    total       = df.count()
    dup_df      = df.groupBy(pk_col).count().filter(col("count") > 1)
    fail_count  = dup_df.agg(_sum("count")).collect()[0][0] or 0
    # subtract one per group (those would be unique representatives)
    dup_groups  = dup_df.count()
    actual_dups = int(fail_count) - dup_groups
    record(dataset_name, "uniqueness", pk_col, actual_dups, total)


# ---------------------------------------------------------------------------
# Check 3: Format — date format validation (multi-format)
# ---------------------------------------------------------------------------
DATE_PATTERNS = [
    r"^\d{4}-\d{2}-\d{2}$",          # YYYY-MM-DD
    r"^\d{2}-\d{2}-\d{4}$",          # DD-MM-YYYY
    r"^\d{2}/\d{2}/\d{2}$",          # MM/DD/YY
]

def check_date_format(df, dataset_name, date_col="Date"):
    print(f"\n--- Date Format ({dataset_name}.{date_col}) ---")
    if date_col not in df.columns:
        print(f"  [SKIP] Column {date_col} not found.")
        return
    total = df.count()
    non_null_df = df.filter(col(date_col).isNotNull() & (trim(col(date_col)) != ""))
    pattern = "|".join(f"(?:{p})" for p in DATE_PATTERNS)
    fail_count = non_null_df.filter(
        regexp_extract(col(date_col), pattern, 0) == ""
    ).count()
    record(dataset_name, "format_date", date_col, fail_count, total)


# ---------------------------------------------------------------------------
# Check 4: Range — Quantity > 0 and Price_per_Unit > 0
# ---------------------------------------------------------------------------
def check_range(df, dataset_name):
    print(f"\n--- Range Checks ({dataset_name}) ---")
    total = df.count()
    for col_name, condition in [
        ("Quantity",       col("Quantity").cast("double") <= 0),
        ("Price_per_Unit", col("Price_per_Unit").cast("double") <= 0),
    ]:
        if col_name not in df.columns:
            continue
        fail_count = df.filter(
            col(col_name).isNotNull() & condition
        ).count()
        record(dataset_name, f"range_{col_name.lower()}", col_name, fail_count, total)


# ---------------------------------------------------------------------------
# Check 5: Consistency — Total_Spent == Price_per_Unit * Quantity
# ---------------------------------------------------------------------------
def check_consistency(df, dataset_name):
    print(f"\n--- Consistency Check ({dataset_name}) ---")
    required = {"Total_Spent", "Price_per_Unit", "Quantity"}
    if not required.issubset(set(df.columns)):
        print(f"  [SKIP] Required columns not all present: {required}")
        return
    total = df.count()
    TOLERANCE = 0.01
    fail_count = df.filter(
        col("Total_Spent").isNotNull() &
        col("Price_per_Unit").isNotNull() &
        col("Quantity").isNotNull()
    ).filter(
        spark_abs(
            col("Total_Spent").cast("double") -
            (col("Price_per_Unit").cast("double") * col("Quantity").cast("double"))
        ) > TOLERANCE
    ).count()
    record(dataset_name, "consistency_total_spent", "Total_Spent", fail_count, total)


# ---------------------------------------------------------------------------
# Check 6: Referential integrity — Customer_IDs in Transactions exist in Customers
# ---------------------------------------------------------------------------
def check_referential(txn_df, cust_df):
    print(f"\n--- Referential Integrity (transactions -> customers) ---")
    total = txn_df.count()
    orphans = txn_df.join(
        cust_df.select("Customer_ID"), on="Customer_ID", how="left_anti"
    ).count()
    record("transactions", "referential_integrity", "Customer_ID", orphans, total)


# ---------------------------------------------------------------------------
# Persist DQ report
# ---------------------------------------------------------------------------
def save_dq_report():
    print(f"\n--- Saving DQ Report ---")
    tbl_path = DQ_PATH.replace("\\", "/")
    # Ensure Delta DQ report table exists using JVM SQL to avoid Python worker crashes
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{tbl_path}` (
            run_id STRING,
            dataset STRING,
            check_name STRING,
            column STRING,
            fail_count LONG,
            fail_rate DOUBLE,
            checked_at STRING
        ) USING delta
    """)
    
    # Insert each row using pure SQL to avoid Python worker crashes
    for r in dq_results:
        dataset = r["dataset"].replace("'", "''")
        check_name = r["check_name"].replace("'", "''")
        col_name = r["column"].replace("'", "''")
        spark.sql(f"""
            INSERT INTO delta.`{tbl_path}` VALUES (
                '{r["run_id"]}', '{dataset}', '{check_name}', '{col_name}',
                {r["fail_count"]}, {r["fail_rate"]}, '{r["checked_at"]}'
            )
        """)
        
    # Read back and show
    dq_df = spark.read.format("delta").load(tbl_path).filter(col("run_id") == RUN_ID)
    dq_df.orderBy("dataset", "check_name").show(50, truncate=False)
    print(f"[INFO]  DQ report saved -> {tbl_path}")
    return dq_df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run():
    print(f"\n{'='*60}")
    print(f"  TrustGuard — Module 2: DQ Checks  |  run_id={RUN_ID}")
    print(f"{'='*60}")

    txn_df  = spark.read.format("delta").load(f"{RAW_PATH}/transactions_raw")
    cust_df = spark.read.format("delta").load(f"{RAW_PATH}/customers_raw")

    check_completeness(txn_df,  "transactions")
    check_completeness(cust_df, "customers")
    check_uniqueness(txn_df,  "transactions", "Transaction_ID")
    check_uniqueness(cust_df, "customers",    "Customer_ID")
    check_date_format(txn_df, "transactions", "Date")
    check_range(txn_df,       "transactions")
    check_consistency(txn_df, "transactions")
    check_referential(txn_df, cust_df)

    dq_df = save_dq_report()
    print(f"\n[INFO]  DQ Checks complete.  Total checks: {len(dq_results)}")
    return dq_df


if __name__ == "__main__":
    run()
