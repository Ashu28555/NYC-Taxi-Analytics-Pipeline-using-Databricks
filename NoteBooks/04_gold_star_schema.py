# Databricks notebook source
# DBTITLE 1,Import PySpark Functions
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Load Fact and Dimension Tables
fact_df = spark.table("taxi_analytics.gold.fact_trips")

date_df = spark.table("taxi_analytics.gold.dim_date")

zone_df = spark.table("taxi_analytics.gold.dim_zone")

payment_df = spark.table("taxi_analytics.gold.dim_payment_type")

# COMMAND ----------

# DBTITLE 1,Aggregate Trip Metrics by Date
trip_summary = (
    fact_df
    .groupBy("date_key")
    .agg(
        count("*").alias("trip_count"),
        sum("total_amount").alias("revenue"),
        avg("fare_amount").alias("avg_fare"),
        avg("trip_distance").alias("avg_distance")
    )
)

# COMMAND ----------

# DBTITLE 1,Join with Date Dimension
trip_summary = (
    trip_summary
    .join(
        date_df.select(
            "date_key",
            "full_date",
            "year",
            "month",
            "month_name"
        ),
        "date_key"
    )
)

# COMMAND ----------

# DBTITLE 1,Save Trip Summary by Day Table
trip_summary.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable(
    "taxi_analytics.gold.trip_summary_by_day"
)

# COMMAND ----------

# DBTITLE 1,Join Facts with Zone for Borough Analysis
revenue_by_borough = (
    fact_df.alias("f")
    .join(
        zone_df.alias("z"),
        col("f.pickup_zone_id")
        ==
        col("z.zone_id")
    )
)

# COMMAND ----------

# DBTITLE 1,Aggregate Revenue by Borough
revenue_by_borough = (
    revenue_by_borough
    .groupBy("borough")
    .agg(
        sum("total_amount")
        .alias("revenue"),

        count("*")
        .alias("trip_count")
    )
)

# COMMAND ----------

# DBTITLE 1,Save Revenue by Borough Table
revenue_by_borough.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable(
"taxi_analytics.gold.revenue_by_borough"
)

# COMMAND ----------

# DBTITLE 1,Join Facts with Zone for Pickup Analysis
top_pickup = (
    fact_df.alias("f")
    .join(
        zone_df.alias("z"),
        col("f.pickup_zone_id")
        ==
        col("z.zone_id")
    )
)

# COMMAND ----------

# DBTITLE 1,Aggregate Pickup Counts by Zone
top_pickup = (
    top_pickup
    .groupBy(
        "borough",
        "zone"
    )
    .agg(
        count("*")
        .alias("trip_count")
    )
)

# COMMAND ----------

# DBTITLE 1,Order Pickup Zones by Trip Count
top_pickup = top_pickup.orderBy(
    col("trip_count").desc()
)

# COMMAND ----------

# DBTITLE 1,Save Top Pickup Zones Table
top_pickup.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable(
"taxi_analytics.gold.top_pickup_zones"
)

# COMMAND ----------

# DBTITLE 1,Join Facts with Payment Dimension
payment_analysis = (
    fact_df.alias("f")
    .join(
        payment_df.alias("p"),
        "payment_key"
    )
)

# COMMAND ----------

# DBTITLE 1,Aggregate Payment Analysis Metrics
payment_analysis = (
    payment_analysis
    .groupBy(
        "payment_description"
    )
    .agg(
        count("*")
        .alias("trip_count"),

        sum("total_amount")
        .alias("revenue")
    )
)

# COMMAND ----------

# DBTITLE 1,Save Payment Analysis Table
payment_analysis.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable(
"taxi_analytics.gold.payment_analysis"
)

# COMMAND ----------

# DBTITLE 1,Load Data Quality Results
dq_summary = (
    spark.table(
        "taxi_analytics.gold.data_quality_results"
    )
)

# COMMAND ----------

# DBTITLE 1,Aggregate Data Quality Summary
dq_summary = (
    dq_summary
    .groupBy("rule_name")
    .agg(
        sum("passed_count")
        .alias("passed_count"),

        sum("failed_count")
        .alias("failed_count")
    )
)

# COMMAND ----------

# DBTITLE 1,Save Data Quality Summary Table
dq_summary.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable(
"taxi_analytics.gold.data_quality_summary"
)

# COMMAND ----------

# DBTITLE 1,Generate Unique Run ID
from uuid import uuid4

run_id = str(uuid4())

# COMMAND ----------

# DBTITLE 1,Log Pipeline Audit Record
spark.sql(f"""
INSERT INTO taxi_analytics.metadata.pipeline_audit
SELECT
'{run_id}',
'05_analytics_mart',
current_timestamp(),
current_timestamp(),
5,
'SUCCESS'
""")