CREATE SCHEMA IF NOT EXISTS taxi_analytics.silver;

CREATE TABLE IF NOT EXISTS taxi_analytics.silver.trips
USING DELTA;
