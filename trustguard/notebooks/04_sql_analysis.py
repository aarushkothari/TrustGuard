# =============================================================================
# TrustGuard — Module 4: SQL Analysis & Final Layer
# =============================================================================
# Goal: Build analysis-ready final tables and business-facing queries.
#       Produce queryable Delta tables + reusable SQL files.
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
    col, sum as _sum, count, avg, max as _max, min as _min,
    round as _round, date_trunc, month, year, concat_ws,
    when, lit, rank
)
from pyspark.sql.window import Window

builder = SparkSession.builder \
    .appName("TrustGuard-SQLAnalysis") \
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
CLEAN_PATH  = f"{BASE_PATH}/data/clean"
FINAL_PATH  = f"{BASE_PATH}/data/final"
SQL_PATH    = f"{BASE_PATH}/sql"
RUN_ID      = str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Step 1: Build final_transactions (join clean tables)
# ---------------------------------------------------------------------------
def build_final_transactions(txn_df, cust_df):
    print("\n--- Step 1: Building final_transactions ---")

    final = txn_df.join(cust_df.select("Customer_ID"), on="Customer_ID", how="inner")
    final = final.withColumn("Date", col("Date").cast("date"))
    final = final.withColumn("Year",  year(col("Date"))) \
                 .withColumn("Month", month(col("Date")))

    final.write.format("delta").mode("overwrite").save(f"{FINAL_PATH}/final_transactions")
    print(f"  final_transactions: {final.count():,} rows")
    final.createOrReplaceTempView("final_transactions")
    return final


# ---------------------------------------------------------------------------
# Step 2: Customer Summary (loyalty tier)
# ---------------------------------------------------------------------------
def build_customer_summary(final_df):
    print("\n--- Step 2: Building customer_summary ---")

    summary = final_df.groupBy("Customer_ID").agg(
        _sum("Total_Spent").alias("total_spend"),
        count("Transaction_ID").alias("order_count"),
        _round(avg("Total_Spent"), 2).alias("avg_order_value"),
        _max("Date").alias("last_purchase_date"),
    )

    # Loyalty tiers
    summary = summary.withColumn("loyalty_tier",
        when(col("total_spend") >= 10000, lit("Platinum"))
        .when(col("total_spend") >= 5000,  lit("Gold"))
        .when(col("total_spend") >= 1000,  lit("Silver"))
        .otherwise(lit("Bronze"))
    )

    summary.write.format("delta").mode("overwrite").save(f"{FINAL_PATH}/customer_summary")
    summary.createOrReplaceTempView("customer_summary")
    print(f"  customer_summary: {summary.count():,} rows")
    print("  Loyalty tier distribution:")
    summary.groupBy("loyalty_tier").count().orderBy("count", ascending=False).show()
    return summary


# ---------------------------------------------------------------------------
# Step 3: City Sales Report (monthly revenue + orders by city)
# ---------------------------------------------------------------------------
def build_city_sales_report(final_df):
    print("\n--- Step 3: Building city_sales_report ---")

    report = final_df.groupBy(
        year(col("Date")).alias("year"),
        month(col("Date")).alias("month"),
        col("Location").alias("city")
    ).agg(
        _round(_sum("Total_Spent"), 2).alias("total_revenue"),
        count("Transaction_ID").alias("order_count"),
        _round(avg("Total_Spent"), 2).alias("avg_order_value"),
    ).orderBy("year", "month", col("total_revenue").desc())

    report.write.format("delta").mode("overwrite").save(f"{FINAL_PATH}/city_sales_report")
    report.createOrReplaceTempView("city_sales_report")
    print(f"  city_sales_report: {report.count():,} rows")
    return report


# ---------------------------------------------------------------------------
# Step 4: Persist DQ report as final-layer table
# ---------------------------------------------------------------------------
def persist_dq_report():
    print("\n--- Step 4: Persisting DQ Report to Final Layer ---")
    try:
        dq_df = spark.read.format("delta").load(f"{CLEAN_PATH}/dq_report")
        dq_df.write.format("delta").mode("overwrite").save(f"{FINAL_PATH}/dq_report")
        print(f"  dq_report persisted: {dq_df.count():,} rows")
    except Exception as e:
        print(f"  [WARN] Could not load dq_report from clean layer: {e}")


# ---------------------------------------------------------------------------
# Step 5: Run and display SQL queries
# ---------------------------------------------------------------------------
def run_sql_analyses():
    print("\n--- Step 5: Running SQL Analyses ---")

    queries = {
        "monthly_revenue_by_city": """
            SELECT year, month, city,
                   ROUND(SUM(total_revenue), 2) AS revenue,
                   SUM(order_count)             AS orders
            FROM city_sales_report
            GROUP BY year, month, city
            ORDER BY year DESC, month DESC, revenue DESC
        """,
        "top_10_customers_by_spend": """
            SELECT Customer_ID, total_spend, order_count, loyalty_tier
            FROM customer_summary
            ORDER BY total_spend DESC
            LIMIT 10
        """,
        "payment_method_share": """
            SELECT Payment_Method,
                   COUNT(*) AS transaction_count,
                   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_share
            FROM final_transactions
            GROUP BY Payment_Method
            ORDER BY transaction_count DESC
        """,
        "category_breakdown": """
            SELECT Category,
                   COUNT(*)                    AS order_count,
                   ROUND(SUM(Total_Spent), 2)  AS total_revenue,
                   ROUND(AVG(Total_Spent), 2)  AS avg_per_order
            FROM final_transactions
            GROUP BY Category
            ORDER BY total_revenue DESC
        """,
    }

    results = {}
    for name, sql in queries.items():
        print(f"\n  Query: {name}")
        result_df = spark.sql(sql.strip())
        result_df.show(15, truncate=False)
        # Write to final layer
        result_df.write.format("delta").mode("overwrite").save(f"{FINAL_PATH}/{name}")
        results[name] = result_df

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run():
    print(f"\n{'='*60}")
    print(f"  TrustGuard — Module 4: SQL Analysis  |  run_id={RUN_ID}")
    print(f"{'='*60}")

    txn_df  = spark.read.format("delta").load(f"{CLEAN_PATH}/transactions_clean")
    cust_df = spark.read.format("delta").load(f"{CLEAN_PATH}/customers_clean")

    final_df = build_final_transactions(txn_df, cust_df)
    build_customer_summary(final_df)
    build_city_sales_report(final_df)
    persist_dq_report()
    run_sql_analyses()

    print(f"\n[INFO]  SQL Analysis complete.")
    return final_df


if __name__ == "__main__":
    run()
