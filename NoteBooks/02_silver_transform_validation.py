# Databricks notebook source
# DBTITLE 1,Import Required Libraries
from pyspark.sql.functions import *
from datetime import datetime
from uuid import uuid4

# COMMAND ----------

# DBTITLE 1,Load Bronze Layer Trip Data
trip_df = spark.table("taxi_analytics.bronze.trips")

# COMMAND ----------

# DBTITLE 1,Count Total Records
print(trip_df.count())

# COMMAND ----------

# DBTITLE 1,Calculate Trip Duration in Minutes
trip_df = trip_df.withColumn(
    "trip_duration_minutes",
    (
        unix_timestamp("lpep_dropoff_datetime")
        -
        unix_timestamp("lpep_pickup_datetime")
    ) / 60
)

# COMMAND ----------

# DBTITLE 1,Create Data Quality Rule Master Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxi_analytics.metadata.rule_master
# MAGIC (
# MAGIC rule_id INT,
# MAGIC rule_name STRING,
# MAGIC rule_description STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Insert Validation Rules into Rule Master
# MAGIC %sql
# MAGIC INSERT INTO taxi_analytics.metadata.rule_master VALUES
# MAGIC (1,'Positive Fare','fare_amount > 0');
# MAGIC
# MAGIC INSERT INTO taxi_analytics.metadata.rule_master VALUES
# MAGIC (2,'Positive Distance','trip_distance > 0');
# MAGIC
# MAGIC INSERT INTO taxi_analytics.metadata.rule_master VALUES
# MAGIC (3,'Valid Passenger Count','passenger_count > 0');
# MAGIC
# MAGIC INSERT INTO taxi_analytics.metadata.rule_master VALUES
# MAGIC (4,'Valid Duration','trip_duration_minutes <= 180');
# MAGIC
# MAGIC INSERT INTO taxi_analytics.metadata.rule_master VALUES
# MAGIC (5,'Pickup Not Null','pickup datetime present');

# COMMAND ----------

# DBTITLE 1,Filter Valid Records by Quality Rules
valid_df = trip_df.filter(
"""
fare_amount > 0
AND trip_distance > 0
AND passenger_count > 0
AND trip_duration_minutes <= 180
AND lpep_pickup_datetime IS NOT NULL
AND lpep_dropoff_datetime IS NOT NULL
"""
)

# COMMAND ----------

# DBTITLE 1,Extract Invalid Records Using Subtract
invalid_df = trip_df.subtract(valid_df)

# COMMAND ----------

# DBTITLE 1,Display Valid vs Invalid Counts
print("Valid Records:", valid_df.count())
print("Invalid Records:", invalid_df.count())

# COMMAND ----------

# DBTITLE 1,Tag Invalid Records with Failure Reason
invalid_df = invalid_df.withColumn(
    "failure_reason",
    when(col("fare_amount") <= 0,
         "Invalid Fare")
    .when(col("trip_distance") <= 0,
          "Invalid Distance")
    .when(col("passenger_count") <= 0,
          "Invalid Passenger Count")
    .when(col("trip_duration_minutes") > 180,
          "Duration > 180 Minutes")
    .otherwise("Unknown")
)

# COMMAND ----------

# DBTITLE 1,Write Failed Records to Gold Layer
invalid_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "taxi_analytics.gold.failed_records"
    )

# COMMAND ----------

# DBTITLE 1,Select Columns for Silver Layer
silver_df = valid_df.select(
    "trip_key",
    "VendorID",
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
    "PULocationID",
    "DOLocationID",
    "trip_duration_minutes",
    "ingestion_timestamp"
)

# COMMAND ----------

# DBTITLE 1,Write Validated Records to Silver Layer
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "taxi_analytics.silver.trips"
    )

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed on Silver Table
# MAGIC %sql
# MAGIC ALTER TABLE taxi_analytics.silver.trips
# MAGIC SET TBLPROPERTIES
# MAGIC (
# MAGIC delta.enableChangeDataFeed=true
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Create Data Quality Results Table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS taxi_analytics.gold.data_quality_results
# MAGIC (
# MAGIC run_id STRING,
# MAGIC rule_name STRING,
# MAGIC passed_count BIGINT,
# MAGIC failed_count BIGINT,
# MAGIC run_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Generate Unique Run ID
run_id = str(uuid4())

# COMMAND ----------

# DBTITLE 1,Count Total Passed and Failed Records
total_records = trip_df.count()
failed_records = invalid_df.count()
passed_records = valid_df.count()

# COMMAND ----------

# DBTITLE 1,Create Data Quality Results DataFrame
dq_df = spark.createDataFrame(
[
(
run_id,
"Overall Validation",
passed_records,
failed_records,
datetime.now()
)
],
[
"run_id",
"rule_name",
"passed_count",
"failed_count",
"run_timestamp"
]
)

# COMMAND ----------

# DBTITLE 1,Write Data Quality Metrics to Gold Layer
dq_df.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable(
        "taxi_analytics.gold.data_quality_results"
    )

# COMMAND ----------

# DBTITLE 1,Log Pipeline Audit Record
spark.sql(f"""
INSERT INTO taxi_analytics.metadata.pipeline_audit
SELECT
'{run_id}',
'02_transform',
current_timestamp(),
current_timestamp(),
{passed_records},
'SUCCESS'
""")