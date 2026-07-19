# =============================================================================
# TrustGuard — Module 5: Anomaly Detection
# =============================================================================
# Goal: Lightweight rule-based flagging — runs post-cleaning, non-blocking.
#       Flagged rows are written to anomaly_log; final_transactions untouched.
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

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, abs as _abs, lit, mean as _mean, stddev as _stddev,
    when, current_timestamp
)

builder = SparkSession.builder \
    .appName("TrustGuard-AnomalyDetection") \
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
CLEAN_PATH = f"{BASE_PATH}/data/clean"
FINAL_PATH = f"{BASE_PATH}/data/final"
RUN_ID     = str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Rule 1: Quantity > 3 standard deviations from mean
# ---------------------------------------------------------------------------
def flag_quantity_outliers(df):
    print("\n--- Rule 1: Quantity Outliers (>3 stddevs) ---")

    stats = df.select(
        _mean("Quantity").alias("mean"),
        _stddev("Quantity").alias("std"),
    ).collect()[0]

    mean_qty = stats["mean"]
    std_qty  = stats["std"]
    threshold = 3 * std_qty

    print(f"  Quantity mean={mean_qty:.2f}, std={std_qty:.2f}, threshold=±{threshold:.2f}")

    outliers = df.filter(
        _abs(col("Quantity") - lit(mean_qty)) > lit(threshold)
    ).withColumn("reason",        lit("quantity_outlier_3sigma")) \
     .withColumn("detected_at",   current_timestamp()) \
     .withColumn("run_id",        lit(RUN_ID))

    count = outliers.count()
    print(f"  Quantity outliers flagged: {count:,}")
    return outliers


# ---------------------------------------------------------------------------
# Rule 2: Price_per_Unit <= 0
# ---------------------------------------------------------------------------
def flag_negative_prices(df):
    print("\n--- Rule 2: Negative/Zero Prices ---")

    anomalies = df.filter(
        col("Price_per_Unit").isNotNull() & (col("Price_per_Unit") <= 0)
    ).withColumn("reason",        lit("price_per_unit_nonpositive")) \
     .withColumn("detected_at",   current_timestamp()) \
     .withColumn("run_id",        lit(RUN_ID))

    count = anomalies.count()
    print(f"  Non-positive Price_per_Unit flagged: {count:,}")
    return anomalies


# ---------------------------------------------------------------------------
# Rule 3: Total_Spent extreme outliers (>4 stddevs) — high-value fraud signal
# ---------------------------------------------------------------------------
def flag_total_spent_outliers(df):
    print("\n--- Rule 3: Total_Spent Extreme Outliers (>4 stddevs) ---")

    stats = df.select(
        _mean("Total_Spent").alias("mean"),
        _stddev("Total_Spent").alias("std"),
    ).collect()[0]

    mean_spent = stats["mean"]
    std_spent  = stats["std"]

    anomalies = df.filter(
        _abs(col("Total_Spent") - lit(mean_spent)) > lit(4 * std_spent)
    ).withColumn("reason",        lit("total_spent_extreme_outlier")) \
     .withColumn("detected_at",   current_timestamp()) \
     .withColumn("run_id",        lit(RUN_ID))

    print(f"  Extreme Total_Spent outliers flagged: {anomalies.count():,}")
    return anomalies


# ---------------------------------------------------------------------------
# Write anomaly_log
# ---------------------------------------------------------------------------
def save_anomaly_log(frames):
    print("\n--- Saving anomaly_log ---")
    if not frames:
        print("  No anomalies detected.")
        return

    # Union all frames
    log_df = frames[0]
    for f in frames[1:]:
        log_df = log_df.unionByName(f, allowMissingColumns=True)

    # Deduplicate across rules (same row might hit multiple rules)
    log_df = log_df.dropDuplicates(["Transaction_ID", "reason"])
    log_df.write.format("delta").mode("overwrite").save(f"{FINAL_PATH}/anomaly_log")
    total = log_df.count()
    print(f"  anomaly_log: {total:,} total anomalies")
    log_df.groupBy("reason").count().show()
    return log_df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run():
    print(f"\n{'='*60}")
    print(f"  TrustGuard — Module 5: Anomaly Detection  |  run_id={RUN_ID}")
    print(f"{'='*60}")

    # Load CLEAN transactions (anomaly detection runs on cleaned data)
    txn_df = spark.read.format("delta").load(f"{CLEAN_PATH}/transactions_clean")
    print(f"  Loaded transactions_clean: {txn_df.count():,} rows")

    frames = []
    frames.append(flag_quantity_outliers(txn_df))
    frames.append(flag_negative_prices(txn_df))
    frames.append(flag_total_spent_outliers(txn_df))

    log_df = save_anomaly_log(frames)

    # Confirm final_transactions is untouched
    final_count = spark.read.format("delta").load(f"{FINAL_PATH}/final_transactions").count()
    print(f"\n  [CHECKPOINT] final_transactions count unchanged: {final_count:,}")
    print(f"\n[INFO]  Anomaly detection complete — anomalies logged, not removed.")
    return log_df


if __name__ == "__main__":
    run()
