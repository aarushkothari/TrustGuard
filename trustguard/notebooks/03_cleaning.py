# =============================================================================
# TrustGuard — Module 3: Data Cleaning
# =============================================================================
# Goal: Fix what Module 2 flagged. Nothing silently dropped —
#       all failures go to `rejected_records` with a reason column.
# =============================================================================

import sys, os, uuid
from datetime import datetime

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

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, lower, when, coalesce, to_date,
    row_number, lit, count, abs as _abs
)
from pyspark.sql.types import StringType
import pandas as pd
from functools import reduce

builder = SparkSession.builder \
    .appName("TrustGuard-Cleaning") \
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
RAW_PATH   = f"{BASE_PATH}/data/raw"
CLEAN_PATH = f"{BASE_PATH}/data/clean"
LOOKUP_PATH= f"{BASE_PATH}/lookups"
RUN_ID     = str(uuid.uuid4())

# Accumulate all rejection DataFrames
rejection_frames = []

# ---------------------------------------------------------------------------
# Helper: tag and collect rejections
# ---------------------------------------------------------------------------
REJECTION_COLS = [
    "Transaction_ID", "Customer_ID", "Category", "Item",
    "Price_per_Unit", "Quantity", "Total_Spent",
    "Payment_Method", "Location", "Date", "reason"
]

def collect_rejected(df, reason_label: str):
    rejected = df.withColumn("reason", lit(reason_label))
    # Keep only known columns
    available = [c for c in REJECTION_COLS if c in rejected.columns] + \
                [c for c in rejected.columns if c == "reason"]
    rejection_frames.append(rejected.select(*set(available)))
    return rejected


# ---------------------------------------------------------------------------
# Step 1: Null handling
# ---------------------------------------------------------------------------
def handle_nulls(df):
    print("\n--- Step 1: Null Handling ---")
    initial = df.count()

    # Fill missing Category
    df = df.withColumn("Category",
        when(col("Category").isNull() | (trim(col("Category")) == ""), lit("Unknown"))
        .otherwise(col("Category"))
    )

    # Route rows with NULL Transaction_ID or Customer_ID to rejected_records
    rejected_mask = col("Transaction_ID").isNull() | (trim(col("Transaction_ID")) == "") | \
                    col("Customer_ID").isNull()    | (trim(col("Customer_ID")) == "")

    rejected = df.filter(rejected_mask)
    df       = df.filter(~rejected_mask)

    if rejected.count() > 0:
        collect_rejected(rejected, "missing_primary_key")

    print(f"  Rows after null handling: {df.count():,} (rejected: {rejected.count():,})")
    return df


# ---------------------------------------------------------------------------
# Step 2: Deduplication — keep earliest Transaction_ID
# ---------------------------------------------------------------------------
def deduplicate(df):
    print("\n--- Step 2: Deduplication ---")
    initial = df.count()

    # Use row_number to keep first occurrence (monotonically_increasing_id as proxy for order)
    from pyspark.sql.functions import monotonically_increasing_id
    df = df.withColumn("_row_id", monotonically_increasing_id())
    w  = Window.partitionBy("Transaction_ID").orderBy("_row_id")
    df = df.withColumn("_rn", row_number().over(w))

    duplicates = df.filter(col("_rn") > 1).drop("_row_id", "_rn")
    df         = df.filter(col("_rn") == 1).drop("_row_id", "_rn")

    dup_count = duplicates.count()
    if dup_count > 0:
        collect_rejected(duplicates, "duplicate_transaction_id")

    print(f"  Duplicates removed: {dup_count:,}")
    print(f"  Rows after dedup  : {df.count():,}")
    return df


# ---------------------------------------------------------------------------
# Step 3: Date standardization — normalize to YYYY-MM-DD
# ---------------------------------------------------------------------------
def standardize_dates(df):
    print("\n--- Step 3: Date Standardization ---")
    before_bad = df.filter(
        col("Date").isNotNull() &
        ~col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$")
    ).count()

    df = df.withColumn("Date", coalesce(
        to_date(col("Date"), "yyyy-MM-dd"),
        to_date(col("Date"), "d-M-yyyy"),
        to_date(col("Date"), "M/d/yy"),
        to_date(col("Date"), "d/M/yyyy"),
        to_date(col("Date"), "yyyy/M/d")
    ).cast("string"))

    # Rows where date could not be parsed -> rejected
    unparseable = df.filter(col("Date").isNull())
    df          = df.filter(col("Date").isNotNull())

    if unparseable.count() > 0:
        collect_rejected(unparseable, "unparseable_date")

    after_bad = df.filter(
        col("Date").isNotNull() & ~col("Date").rlike(r"^\d{4}-\d{2}-\d{2}$")
    ).count()
    print(f"  Non-standard dates before: {before_bad:,} -> after: {after_bad:,}")
    return df


# ---------------------------------------------------------------------------
# Step 4: Text normalization
# ---------------------------------------------------------------------------
PAYMENT_MAP = {
    "credit card": "Credit Card", "creditcard": "Credit Card", "cc": "Credit Card",
    "debit card": "Debit Card",  "debitcard": "Debit Card",   "dc": "Debit Card",
    "upi": "UPI", "u.p.i.": "UPI", "u.p.i": "UPI",
    "cash": "Cash",
    "net banking": "Net Banking", "netbanking": "Net Banking",
}

def normalize_text(df):
    print("\n--- Step 4: Text Normalization ---")
    # Payment_Method
    if "Payment_Method" in df.columns:
        mapping_expr = col("Payment_Method")
        for raw_val, canonical in PAYMENT_MAP.items():
            mapping_expr = when(
                lower(trim(col("Payment_Method"))) == raw_val, lit(canonical)
            ).otherwise(mapping_expr)
        df = df.withColumn("Payment_Method", mapping_expr)

    print(f"  Payment_Method values: {[r[0] for r in df.select('Payment_Method').distinct().collect()]}")
    return df


# ---------------------------------------------------------------------------
# Step 5: City correction via lookup table
# ---------------------------------------------------------------------------
def apply_city_corrections(df):
    print("\n--- Step 5: City Corrections ---")
    lookup_path = f"{LOOKUP_PATH}/city_corrections.csv"
    try:
        lookup_df = spark.read.option("header", "true").csv(lookup_path)
        df = df.join(
            lookup_df.withColumnRenamed("raw_value", "Location")
                     .withColumnRenamed("correct_value", "_corrected_location"),
            on="Location", how="left"
        )
        df = df.withColumn("Location",
            coalesce(col("_corrected_location"), col("Location"))
        ).drop("_corrected_location")

        unmatched = df.filter(col("Location").isNull()).count()
        if unmatched > 0:
            print(f"  [WARN] {unmatched:,} rows with unmatched/null Location after correction")
    except Exception as e:
        print(f"  [WARN] Could not load city_corrections.csv: {e}. Skipping.")
    return df


# ---------------------------------------------------------------------------
# Step 6: Type casting
# ---------------------------------------------------------------------------
def cast_types(df):
    print("\n--- Step 6: Type Casting ---")
    cast_map = {
        "Quantity":       "integer",
        "Price_per_Unit": "double",
        "Total_Spent":    "double",
    }
    for col_name, dtype in cast_map.items():
        if col_name not in df.columns:
            continue
        orig_non_null = df.filter(col(col_name).isNotNull()).count()
        df = df.withColumn(col_name, col(col_name).cast(dtype))
        cast_nulls = df.filter(
            col(col_name).isNull()
        ).count()
        cast_failures = max(0, cast_nulls - (df.count() - orig_non_null))
        if cast_failures > 0:
            failed_df = df.filter(col(col_name).isNull() & lit(True))
            collect_rejected(failed_df, f"type_cast_failure_{col_name.lower()}")
            df = df.filter(col(col_name).isNotNull())
            print(f"  Cast failures for {col_name}: {cast_failures:,}")
        else:
            print(f"  {col_name} cast to {dtype} — OK")
    return df


# ---------------------------------------------------------------------------
# Step 7: Amount validation (flag, don't reject)
# ---------------------------------------------------------------------------
def validate_amounts(df):
    print("\n--- Step 7: Amount Validation ---")
    if {"Total_Spent", "Price_per_Unit", "Quantity"}.issubset(set(df.columns)):
        TOLERANCE = 0.01
        df = df.withColumn("total_amount_mismatch",
            when(
                _abs(col("Total_Spent") - (col("Price_per_Unit") * col("Quantity"))) > TOLERANCE,
                lit(True)
            ).otherwise(lit(False))
        )
        mismatches = df.filter(col("total_amount_mismatch")).count()
        print(f"  Amount mismatches flagged: {mismatches:,} (kept with flag, not rejected)")
    return df


# ---------------------------------------------------------------------------
# Step 8: Referential integrity — orphaned Customer_IDs
# ---------------------------------------------------------------------------
def check_referential_integrity(txn_df, cust_df):
    print("\n--- Step 8: Referential Integrity ---")
    orphans = txn_df.join(
        cust_df.select("Customer_ID"), on="Customer_ID", how="left_anti"
    )
    orphan_count = orphans.count()
    if orphan_count > 0:
        collect_rejected(orphans, "customer_not_found")
        txn_df = txn_df.join(
            cust_df.select("Customer_ID"), on="Customer_ID", how="inner"
        )
        print(f"  Orphaned transactions rejected: {orphan_count:,}")
    else:
        print(f"  No orphaned Customer_IDs found.")
    return txn_df


# ---------------------------------------------------------------------------
# Save outputs
# ---------------------------------------------------------------------------
def save_outputs(clean_txn, clean_cust, raw_count):
    print("\n--- Saving Clean Tables ---")

    clean_txn.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}/transactions_clean")
    clean_cust.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}/customers_clean")
    print(f"  transactions_clean written: {clean_txn.count():,} rows")
    print(f"  customers_clean  written  : {clean_cust.count():,} rows")

    if rejection_frames:
        from functools import reduce
        from pyspark.sql import DataFrame
        all_rejections = reduce(DataFrame.unionByName, rejection_frames, rejection_frames[0])
        # Remove duplicate accumulation
        all_rejections = rejection_frames[0]
        for f in rejection_frames[1:]:
            all_rejections = all_rejections.unionByName(f, allowMissingColumns=True)
        all_rejections.write.format("delta").mode("overwrite").save(f"{CLEAN_PATH}/rejected_records")
        rejected_count = all_rejections.count()
        print(f"  rejected_records written  : {rejected_count:,} rows")

        # Reconciliation check
        clean_count = clean_txn.count()
        print(f"\n  [RECONCILIATION] raw={raw_count:,} | clean={clean_count:,} | rejected={rejected_count:,}")
        if raw_count == clean_count + rejected_count:
            print("  [OK] Row counts reconcile.")
        else:
            diff = raw_count - (clean_count + rejected_count)
            print(f"  [WARN] {diff:,} rows unaccounted for — investigate before proceeding!")
    else:
        print("  [INFO] No records rejected.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run():
    print(f"\n{'='*60}")
    print(f"  TrustGuard — Module 3: Cleaning  |  run_id={RUN_ID}")
    print(f"{'='*60}")

    txn_df  = spark.read.format("delta").load(f"{RAW_PATH}/transactions_raw")
    cust_df = spark.read.format("delta").load(f"{RAW_PATH}/customers_raw")
    raw_count = txn_df.count()

    txn_df = handle_nulls(txn_df)
    txn_df = deduplicate(txn_df)
    txn_df = standardize_dates(txn_df)
    txn_df = normalize_text(txn_df)
    txn_df = apply_city_corrections(txn_df)
    txn_df = cast_types(txn_df)
    txn_df = validate_amounts(txn_df)
    txn_df = check_referential_integrity(txn_df, cust_df)

    save_outputs(txn_df, cust_df, raw_count)

    print(f"\n[INFO]  Cleaning complete.")
    return txn_df, cust_df


if __name__ == "__main__":
    run()
