# =============================================================================
# TrustGuard — Module 1: Data Ingestion
# =============================================================================
# Goal: Load source CSVs as-is (all columns as STRING), validate schema,
#       write to raw Delta tables, log ingestion metadata.
# =============================================================================

import sys
import os
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
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit
import uuid

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
builder = SparkSession.builder \
    .appName("TrustGuard-Ingestion") \
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

SOURCE_PATH  = f"{BASE_PATH}/data/raw/source"
RAW_PATH     = f"{BASE_PATH}/data/raw"
METADATA_TBL = f"{RAW_PATH}/ingestion_metadata"

RUN_ID = str(uuid.uuid4())

# ---------------------------------------------------------------------------
# Expected columns (verified against actual CSV on Day 1)
# ---------------------------------------------------------------------------
EXPECTED_COLUMNS = {
    "Transaction_ID",
    "Customer_ID",
    "Category",
    "Item",
    "Price_per_Unit",
    "Quantity",
    "Total_Spent",
    "Payment_Method",
    "Location",
    "Date",
}

# ---------------------------------------------------------------------------
# Helper: Schema validation
# ---------------------------------------------------------------------------
def validate_schema(df, expected_cols: set, dataset_name: str):
    actual = set(df.columns)
    missing = expected_cols - actual
    extra   = actual - expected_cols
    if missing:
        print(f"[ERROR] [{dataset_name}] Missing columns: {missing}")
        raise ValueError(f"Schema mismatch — missing columns: {missing}")
    if extra:
        print(f"[WARN]  [{dataset_name}] Unexpected extra columns: {extra}")
    print(f"[INFO]  [{dataset_name}] Schema check passed.")


# ---------------------------------------------------------------------------
# Helper: Build an all-STRING StructType from expected columns
# ---------------------------------------------------------------------------
def build_string_schema(columns):
    return StructType([StructField(c, StringType(), True) for c in sorted(columns)])


# ---------------------------------------------------------------------------
# Helper: Log ingestion metadata
# ---------------------------------------------------------------------------
def log_metadata(run_id, file_name, table_name, row_count):
    tbl_path = METADATA_TBL.replace("\\", "/")
    # Ensure Delta metadata table exists using JVM SQL to avoid Python worker crashes
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{tbl_path}` (
            run_id STRING,
            file_name STRING,
            table_name STRING,
            row_count LONG,
            loaded_at STRING
        ) USING delta
    """)
    # Insert row using pure SQL
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    spark.sql(f"""
        INSERT INTO delta.`{tbl_path}` VALUES (
            '{run_id}', '{file_name}', '{table_name}', {row_count}, '{loaded_at}'
        )
    """)
    print(f"[INFO]  Metadata logged -> {table_name}: {row_count:,} rows")


# ---------------------------------------------------------------------------
# Step 1: Ingest transactions
# ---------------------------------------------------------------------------
def ingest_transactions(source_file: str):
    print("\n=== STEP 1: Ingesting Transactions ===")
    csv_path = f"{SOURCE_PATH}/{source_file}"

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(csv_path)

    # Rename columns to standard underscore format for downstream compatibility
    column_mapping = {
        "Transaction ID": "Transaction_ID",
        "Customer ID": "Customer_ID",
        "Price Per Unit": "Price_per_Unit",
        "Total Spent": "Total_Spent",
        "Payment Method": "Payment_Method",
        "Transaction Date": "Date"
    }
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    validate_schema(df, EXPECTED_COLUMNS, "transactions")

    # Select only expected columns to drop extra columns containing invalid characters (e.g. spaces) for Delta
    df = df.select(*[c for c in df.columns if c in EXPECTED_COLUMNS])

    out_path = f"{RAW_PATH}/transactions_raw"
    df.write.format("delta").mode("overwrite").save(out_path)
    row_count = df.count()
    log_metadata(RUN_ID, source_file, "transactions_raw", row_count)
    print(f"[INFO]  transactions_raw written -> {out_path} ({row_count:,} rows)")
    return df


# ---------------------------------------------------------------------------
# Step 2: Derive customers from transactions
# ---------------------------------------------------------------------------
def ingest_customers(transactions_df):
    print("\n=== STEP 2: Deriving Customers Table ===")

    # Extract unique Customer_IDs — enrich with Location if it's customer-level
    customer_cols = ["Customer_ID"]
    if "Location" in transactions_df.columns:
        customer_cols.append("Location")

    customers_df = transactions_df.select(*customer_cols).dropDuplicates(["Customer_ID"])
    customers_df = customers_df.withColumn("created_at", current_timestamp())

    out_path = f"{RAW_PATH}/customers_raw"
    customers_df.write.format("delta").mode("overwrite").save(out_path)
    row_count = customers_df.count()
    log_metadata(RUN_ID, "derived_from_transactions", "customers_raw", row_count)
    print(f"[INFO]  customers_raw written -> {out_path} ({row_count:,} rows)")
    return customers_df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(source_file="retail_store_sales.csv"):
    print(f"\n{'='*60}")
    print(f"  TrustGuard — Module 1: Ingestion  |  run_id={RUN_ID}")
    print(f"{'='*60}\n")

    transactions_df = ingest_transactions(source_file)
    customers_df    = ingest_customers(transactions_df)

    # Quick sanity checks
    print(f"\n[CHECKPOINT] transactions_raw row count : {transactions_df.count():,}")
    print(f"[CHECKPOINT] customers_raw   row count : {customers_df.count():,}")
    print(f"\n[INFO]  Ingestion complete.")
    return transactions_df, customers_df


if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else "retail_store_sales.csv"
    run(source)
