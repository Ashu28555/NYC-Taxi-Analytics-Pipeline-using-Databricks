# Databricks notebook source
# DBTITLE 1,Import Libraries
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Define Configuration Constants
CATALOG = "taxi_analytics"

BRONZE_SCHEMA = "bronze"

TRIP_FILE = dbutils.widgets.get("TRIP_FILE")
ZONE_FILE = dbutils.widgets.get("ZONE_FILE")

# COMMAND ----------

# DBTITLE 1,Load Trip Data from Parquet
trip_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .parquet(TRIP_FILE)
)

# COMMAND ----------

# DBTITLE 1,Inspect Trip Data Schema and Sample
trip_df.printSchema()

trip_df.show(5)

# COMMAND ----------

# DBTITLE 1,Display Trip Data
trip_df.display()

# COMMAND ----------

from pyspark.sql.functions import to_date, count

daily_counts_df = (
    trip_df
    .groupBy(to_date("lpep_pickup_datetime").alias("pickup_date"))
    .agg(count("*").alias("record_count"))
    .orderBy("pickup_date")
)

display(daily_counts_df)

# COMMAND ----------

# DBTITLE 1,Add Trip Surrogate Key
trip_df = trip_df.withColumn(
    "trip_key",
    sha2(
        concat_ws(
            "|",
            col("VendorID"),
            col("lpep_pickup_datetime"),
            col("PULocationID"),
            col("DOLocationID")
        ),
        256
    )
)

# COMMAND ----------

# DBTITLE 1,Add Metadata Columns
trip_df = (
    trip_df
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
    .withColumn(
        "source_file",
        lit(TRIP_FILE)
    )
)

# COMMAND ----------

# DBTITLE 1,Write Trip Data to Bronze Table
trip_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("taxi_analytics.bronze.trips")

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed on Trips Table
spark.sql("""
ALTER TABLE taxi_analytics.bronze.trips
SET TBLPROPERTIES (
delta.enableChangeDataFeed = true
)
""")

# COMMAND ----------

# DBTITLE 1,Verify Trips Table Properties
spark.sql("""
SHOW TBLPROPERTIES taxi_analytics.bronze.trips
""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Load Zone Lookup Data from CSV
zone_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(ZONE_FILE)
)

# COMMAND ----------

# DBTITLE 1,Inspect Zone Lookup Data
zone_df.show(5)

# COMMAND ----------

# DBTITLE 1,Write Zone Data to Bronze Table
zone_df.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable("taxi_analytics.bronze.zone_lookup")

# COMMAND ----------

# DBTITLE 1,Enable Change Data Feed on Zone Lookup Table
spark.sql("""
ALTER TABLE taxi_analytics.bronze.zone_lookup
SET TBLPROPERTIES (
delta.enableChangeDataFeed = true
)
""")

# COMMAND ----------

# DBTITLE 1,Create Pipeline Audit Metadata Table
spark.sql("""
CREATE TABLE IF NOT EXISTS taxi_analytics.metadata.pipeline_audit
(
run_id STRING,
pipeline_name STRING,
start_time TIMESTAMP,
end_time TIMESTAMP,
rows_processed BIGINT,
status STRING
)
USING DELTA
""")

# COMMAND ----------

# DBTITLE 1,Log Pipeline Execution Audit Record
from uuid import uuid4

run_id = str(uuid4())
rows_processed = trip_df.count()

spark.sql(f"""
INSERT INTO taxi_analytics.metadata.pipeline_audit
SELECT
'{run_id}',
'01_extract',
current_timestamp(),
current_timestamp(),
{rows_processed},
'SUCCESS'
""")