# =============================================================================
# TrustGuard — Pipeline Driver (run_pipeline.py)
# =============================================================================
# Chains all 5 modules in sequence with full pipeline_log audit tracking.
# Wrap each module in try/except so a single failure is logged, not crashed.
# =============================================================================
#cd trustguard/notebooks
#python run_pipeline.py retail_store_sales.csv
# =============================================================================

import sys, os, uuid, traceback
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

builder = SparkSession.builder \
    .appName("TrustGuard-Pipeline") \
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
LOG_PATH   = f"{BASE_PATH}/data/final/pipeline_log"
RUN_ID     = str(uuid.uuid4())

# Import modules (assumes they live in the same directory / are on sys.path)
sys.path.insert(0, os.path.dirname(__file__))
import importlib

MODULE_NAMES = [
    "01_ingestion",
    "02_dq_checks",
    "03_cleaning",
    "04_sql_analysis",
    "05_anomaly_detection",
]

LAYER_MAP = {
    "01_ingestion":          ("raw",   "ingestion"),
    "02_dq_checks":          ("raw",   "dq_checks"),
    "03_cleaning":           ("clean", "cleaning"),
    "04_sql_analysis":       ("final", "sql_analysis"),
    "05_anomaly_detection":  ("final", "anomaly_detection"),
}

# ---------------------------------------------------------------------------
# Pipeline log utilities
# ---------------------------------------------------------------------------
log_rows = []

def log_stage(module_name, layer, table_name, records_in=None,
              records_out=None, records_rejected=None,
              status="SUCCESS", error_message=None):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    row = (
        RUN_ID, now, module_name, layer, table_name,
        records_in, records_out, records_rejected,
        status, error_message or "",
    )
    log_rows.append(row)
    status_tag = "[OK]" if status == "SUCCESS" else "[FAIL]"
    print(f"  {status_tag} [{status}] {module_name} | {table_name} | in={records_in} out={records_out} rejected={records_rejected}")


def flush_pipeline_log():
    tbl_path = LOG_PATH.replace("\\", "/")
    # Ensure Delta pipeline log table exists using JVM SQL to avoid Python worker crashes
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta.`{tbl_path}` (
            run_id STRING,
            run_timestamp TIMESTAMP,
            module STRING,
            layer STRING,
            table_name STRING,
            records_in INT,
            records_out INT,
            records_rejected INT,
            status STRING,
            error_message STRING
        ) USING delta
    """)
    
    # Insert each row using pure SQL to avoid Python worker crashes
    for row in log_rows:
        run_id, now, module_name, layer, table_name, records_in, records_out, records_rejected, status, error_message = row
        
        rec_in_val = records_in if records_in is not None else "NULL"
        rec_out_val = records_out if records_out is not None else "NULL"
        rec_rej_val = records_rejected if records_rejected is not None else "NULL"
        
        error_msg_escaped = error_message.replace("'", "''")
        timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        
        spark.sql(f"""
            INSERT INTO delta.`{tbl_path}` VALUES (
                '{run_id}', CAST('{timestamp_str}' AS TIMESTAMP), '{module_name}', '{layer}', '{table_name}',
                {rec_in_val}, {rec_out_val}, {rec_rej_val}, '{status}', '{error_msg_escaped}'
            )
        """)
        
    # Read back and show
    from pyspark.sql.functions import col
    df = spark.read.format("delta").load(tbl_path).filter(col("run_id") == RUN_ID)
    df.show(truncate=False)
    print(f"\n[INFO]  Pipeline log flushed -> {tbl_path}")


# ---------------------------------------------------------------------------
# Run each module safely
# ---------------------------------------------------------------------------
def run_module(module_name: str, source_file: str = "retail_store_sales.csv"):
    layer, table_name = LAYER_MAP.get(module_name, ("unknown", module_name))
    print(f"\n{'-'*60}")
    print(f"  Running: {module_name}")
    print(f"{'-'*60}")
    try:
        # Dynamically import and execute the module's run() function
        mod = importlib.import_module(module_name.replace("-", "_"))
        if module_name == "01_ingestion":
            result = mod.run(source_file)
        else:
            result = mod.run()
        log_stage(module_name, layer, table_name, status="SUCCESS")
        return result
    except Exception as e:
        tb = traceback.format_exc()
        print(f"\n[ERROR] {module_name} FAILED:\n{tb}")
        log_stage(module_name, layer, table_name, status="FAILED", error_message=str(e)[:500])
        return None


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------
def main():
    print(f"\n{'='*60}")
    print(f"  TrustGuard Pipeline Runner")
    print(f"  run_id    : {RUN_ID}")
    print(f"  started   : {datetime.now(timezone.utc).replace(tzinfo=None).isoformat()}")
    print(f"  base_path : {BASE_PATH}")
    print(f"{'='*60}")

    source_file = sys.argv[1] if len(sys.argv) > 1 else "retail_store_sales.csv"
    print(f"  source_file: {source_file}")

    for mod_name in MODULE_NAMES:
        run_module(mod_name, source_file)

    flush_pipeline_log()

    print(f"\n{'='*60}")
    print(f"  Pipeline complete!")
    print(f"  run_id: {RUN_ID}")
    print(f"  ended : {datetime.now(timezone.utc).replace(tzinfo=None).isoformat()}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
