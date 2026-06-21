# Databricks notebook source
# DBTITLE 1,Import PySpark Functions and Window
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# COMMAND ----------

# DBTITLE 1,Load Silver Trips and Zone Lookup Tables
silver_df = spark.table("taxi_analytics.silver.trips")

zone_df = spark.table("taxi_analytics.bronze.zone_lookup")

# COMMAND ----------

# DBTITLE 1,Create Zone Dimension with Distinct Locations
dim_zone = zone_df.select(
    col("LocationID").alias("zone_id"),
    col("Borough"),
    col("Zone"),
    col("service_zone")
).distinct()

# COMMAND ----------

# DBTITLE 1,Write Zone Dimension to Gold Layer
dim_zone.write \
.format("delta") \
.mode("append") \
.saveAsTable("taxi_analytics.gold.dim_zone")

# Remove duplicates, keeping only updated data
deduped_zone = spark.table("taxi_analytics.gold.dim_zone") \
    .dropDuplicates(["zone_id"])

deduped_zone.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("taxi_analytics.gold.dim_zone")

# COMMAND ----------

# DBTITLE 1,Display Zone Dimension Sample
spark.table("taxi_analytics.gold.dim_zone").show(10)

# COMMAND ----------

# DBTITLE 1,Create Payment Dimension with Descriptions
dim_payment = (
    silver_df
    .select("payment_type")
    .distinct()
)

dim_payment = dim_payment.withColumn(
    "payment_description",
    when(col("payment_type")==1,"Credit Card")
    .when(col("payment_type")==2,"Cash")
    .when(col("payment_type")==3,"No Charge")
    .when(col("payment_type")==4,"Dispute")
    .when(col("payment_type")==5,"Unknown")
    .when(col("payment_type")==6,"Voided Trip")
    .otherwise("Other")
)

# COMMAND ----------

# DBTITLE 1,Add Payment Key Using Window Function
window_spec = Window.orderBy("payment_type")

dim_payment = dim_payment.withColumn(
    "payment_key",
    row_number().over(window_spec)
)

# COMMAND ----------

# DBTITLE 1,Select Payment Dimension Columns
dim_payment = dim_payment.select(
    "payment_key",
    "payment_type",
    "payment_description"
)

# COMMAND ----------

# DBTITLE 1,Write Payment Dimension to Gold Layer
dim_payment.write \
.mode("append") \
.format("delta") \
.saveAsTable("taxi_analytics.gold.dim_payment_type")

# Remove duplicates, keeping only updated data
deduped_payment = spark.table("taxi_analytics.gold.dim_payment_type") \
    .dropDuplicates(["payment_key"])

deduped_payment.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("taxi_analytics.gold.dim_payment_type")

# COMMAND ----------

# DBTITLE 1,Display Payment Dimension Sample
spark.table("taxi_analytics.gold.dim_payment_type").show(10)

# COMMAND ----------

# DBTITLE 1,Extract Distinct Pickup Dates for Date Dimension
dim_date = (
    silver_df
    .select(
        to_date("lpep_pickup_datetime")
        .alias("full_date")
    )
    .distinct()
)

# COMMAND ----------

# DBTITLE 1,Add Date Attributes and Hierarchies
dim_date = (
    dim_date
    .withColumn(
        "date_key",
        date_format(
            col("full_date"),
            "yyyyMMdd"
        ).cast("int")
    )
    .withColumn(
        "year",
        year("full_date")
    )
    .withColumn(
        "quarter",
        quarter("full_date")
    )
    .withColumn(
        "month",
        month("full_date")
    )
    .withColumn(
        "month_name",
        date_format(
            col("full_date"),
            "MMMM"
        )
    )
    .withColumn(
        "day",
        dayofmonth("full_date")
    )
    .withColumn(
        "weekday",
        date_format(
            col("full_date"),
            "EEEE"
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Write Date Dimension to Gold Layer
dim_date.write \
.mode("append") \
.format("delta") \
.saveAsTable("taxi_analytics.gold.dim_date")

# Remove duplicates, keeping only updated data
deduped_date = spark.table("taxi_analytics.gold.dim_date") \
    .dropDuplicates(["date_key"])

deduped_date.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("taxi_analytics.gold.dim_date")

# COMMAND ----------

# DBTITLE 1,Display Date Dimension Sample
spark.table("taxi_analytics.gold.dim_date").show(5)

# COMMAND ----------

# DBTITLE 1,Add Date Key to Fact Table
fact_df = silver_df.withColumn(
    "date_key",
    date_format(
        to_date("lpep_pickup_datetime"),
        "yyyyMMdd"
    ).cast("int")
)

# COMMAND ----------

# DBTITLE 1,Load Payment Dimension for Join
payment_dim = spark.table(
    "taxi_analytics.gold.dim_payment_type"
)

# COMMAND ----------

# DBTITLE 1,Join Fact Table with Payment Dimension
fact_df = fact_df.join(
    payment_dim,
    "payment_type",
    "left"
)

# COMMAND ----------

# DBTITLE 1,Select Fact Table Columns with Foreign Keys
fact_df = fact_df.select(
    "trip_key",
    "date_key",

    col("PULocationID")
      .alias("pickup_zone_id"),

    col("DOLocationID")
      .alias("dropoff_zone_id"),

    "payment_key",

    "passenger_count",

    "trip_distance",

    "fare_amount",

    "tip_amount",

    "total_amount",

    "trip_duration_minutes"
)

# COMMAND ----------

# DBTITLE 1,Write Fact Trips to Gold Layer
fact_df.write \
.format("delta") \
.mode("append") \
.saveAsTable(
    "taxi_analytics.gold.fact_trips"
)

# Remove duplicates, keeping only updated data
deduped_fact = spark.table("taxi_analytics.gold.fact_trips") \
    .dropDuplicates(["trip_key"])

deduped_fact.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("taxi_analytics.gold.fact_trips")

# COMMAND ----------

# DBTITLE 1,Count Records in Fact Trips
print(
    spark.table(
        "taxi_analytics.gold.fact_trips"
    ).count()
)

# COMMAND ----------

# DBTITLE 1,Count Records in Zone Dimension
print(
    spark.table(
        "taxi_analytics.gold.dim_zone"
    ).count()
)

# COMMAND ----------

# DBTITLE 1,Count Records in Date Dimension
print(
    spark.table(
        "taxi_analytics.gold.dim_date"
    ).count()
)

# COMMAND ----------

# DBTITLE 1,Count Records in Payment Dimension
print(
    spark.table(
        "taxi_analytics.gold.dim_payment_type"
    ).count()
)

# COMMAND ----------

# DBTITLE 1,Generate Unique Run ID for Audit
from uuid import uuid4

run_id = str(uuid4())

# COMMAND ----------

# DBTITLE 1,Count Fact Records for Audit
fact_count = spark.table(
    "taxi_analytics.gold.fact_trips"
).count()

# COMMAND ----------

# DBTITLE 1,Log Gold Load Pipeline Audit Record
spark.sql(f"""
INSERT INTO taxi_analytics.metadata.pipeline_audit
SELECT
'{run_id}',
'04_gold_load',
current_timestamp(),
current_timestamp(),
{fact_count},
'SUCCESS'
""")

# COMMAND ----------

spark.table("taxi_analytics.gold.dim_date") \
    .coalesce(1) \
    .write \
    .mode("append") \
    .option("header", "true") \
    .csv("/Volumes/taxi_analytics/gold/export/dim_date")

# COMMAND ----------

