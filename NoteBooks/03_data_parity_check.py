# Databricks notebook source
# DBTITLE 1,Import Absolute Value Function
from pyspark.sql.functions import abs

# COMMAND ----------

# DBTITLE 1,Load Bronze and Silver Layer Tables
bronze_df = spark.table("taxi_analytics.bronze.trips")

silver_df = spark.table("taxi_analytics.silver.trips")

# COMMAND ----------

# DBTITLE 1,Create Data Parity Summary Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxi_analytics.gold.data_parity_summary
# MAGIC (
# MAGIC run_id STRING,
# MAGIC check_name STRING,
# MAGIC source_value DOUBLE,
# MAGIC target_value DOUBLE,
# MAGIC status STRING,
# MAGIC run_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Create Data Parity Mismatch Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxi_analytics.gold.data_parity_mismatch
# MAGIC (
# MAGIC run_id STRING,
# MAGIC check_name STRING,
# MAGIC source_value DOUBLE,
# MAGIC target_value DOUBLE,
# MAGIC difference DOUBLE,
# MAGIC run_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Generate Unique Run ID
from uuid import uuid4
from datetime import datetime

run_id = str(uuid4())

# COMMAND ----------

# DBTITLE 1,Count Records in Both Layers
bronze_count = bronze_df.count()

silver_count = silver_df.count()

# COMMAND ----------

# DBTITLE 1,Validate Row Count Parity
count_status = (
    "PASS"
    if bronze_count >= silver_count
    else "FAIL"
)

# COMMAND ----------

# DBTITLE 1,Calculate Bronze Layer Total Revenue
bronze_revenue = bronze_df.agg(
    {"total_amount":"sum"}
).collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Calculate Silver Layer Total Revenue
silver_revenue = silver_df.agg(
    {"total_amount":"sum"}
).collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Validate Revenue Parity
revenue_status = (
    "PASS"
    if -1 < bronze_revenue-silver_revenue < 1
    else "FAIL"
)

# COMMAND ----------

# DBTITLE 1,Calculate Bronze Layer Total Distance
bronze_distance = bronze_df.agg(
    {"trip_distance":"sum"}
).collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Calculate Silver Layer Total Distance
silver_distance = silver_df.agg(
    {"trip_distance":"sum"}
).collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Validate Distance Parity
distance_status = (
    "PASS"
    if -1 < bronze_distance-silver_distance < 1
    else "FAIL"
)

# COMMAND ----------

# DBTITLE 1,Build Parity Check Results List
results = [
(
run_id,
"Row Count",
float(bronze_count),
float(silver_count),
count_status,
datetime.now()
),
(
run_id,
"Total Revenue",
float(bronze_revenue),
float(silver_revenue),
revenue_status,
datetime.now()
),
(
run_id,
"Trip Distance",
float(bronze_distance),
float(silver_distance),
distance_status,
datetime.now()
)
]

# COMMAND ----------

# DBTITLE 1,Create Parity Results DataFrame
parity_df = spark.createDataFrame(
results,
[
"run_id",
"check_name",
"source_value",
"target_value",
"status",
"run_timestamp"
]
)

# COMMAND ----------

# DBTITLE 1,Write Parity Summary to Gold Layer
parity_df.write \
.mode("append") \
.format("delta") \
.saveAsTable(
"taxi_analytics.gold.data_parity_summary"
)

# COMMAND ----------

# DBTITLE 1,Filter Failed Parity Checks
mismatch_df = parity_df.filter(
"status='FAIL'"
)

# COMMAND ----------

# DBTITLE 1,Calculate Difference for Mismatches
mismatch_df = mismatch_df.withColumn(
    "difference",
    abs(
        mismatch_df.source_value
        -
        mismatch_df.target_value
    )
)

# COMMAND ----------

# DBTITLE 1,Write Mismatches to Gold Layer
mismatch_df.select(
    "run_id",
    "check_name",
    "source_value",
    "target_value",
    "difference",
    "run_timestamp"
).write \
.mode("append") \
.format("delta") \
.saveAsTable(
"taxi_analytics.gold.data_parity_mismatch"
)

# COMMAND ----------

# DBTITLE 1,Log Pipeline Audit Record
spark.sql(f"""
INSERT INTO taxi_analytics.metadata.pipeline_audit
SELECT
'{run_id}',
'03_data_parity',
current_timestamp(),
current_timestamp(),
3,
'SUCCESS'
""")

# COMMAND ----------

# DBTITLE 1,Display Parity Summary Results
spark.table(
"taxi_analytics.gold.data_parity_summary"
).show(truncate=False)