CREATE TABLE IF NOT EXISTS taxi_analytics.gold.trip_summary_by_day
USING DELTA;


CREATE TABLE IF NOT EXISTS taxi_analytics.gold.revenue_by_borough
USING DELTA;


CREATE TABLE IF NOT EXISTS taxi_analytics.gold.payment_analysis
USING DELTA;


CREATE TABLE IF NOT EXISTS taxi_analytics.gold.top_pickup_zones
USING DELTA;
