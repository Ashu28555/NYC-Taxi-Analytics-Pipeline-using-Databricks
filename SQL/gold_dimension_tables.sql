CREATE SCHEMA IF NOT EXISTS taxi_analytics.gold;

CREATE TABLE IF NOT EXISTS taxi_analytics.gold.dim_date
(
    date_key INT,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name STRING,
    day INT,
    weekday STRING
)
USING DELTA;


CREATE TABLE IF NOT EXISTS taxi_analytics.gold.dim_zone
(
    zone_id INT,
    borough STRING,
    zone STRING,
    service_zone STRING
)
USING DELTA;


CREATE TABLE IF NOT EXISTS taxi_analytics.gold.dim_payment_type
(
    payment_key INT,
    payment_type INT,
    payment_description STRING
)
USING DELTA;
