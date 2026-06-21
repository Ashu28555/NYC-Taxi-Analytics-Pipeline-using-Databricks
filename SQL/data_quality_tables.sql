CREATE TABLE IF NOT EXISTS taxi_analytics.gold.failed_records
USING DELTA;


CREATE TABLE IF NOT EXISTS taxi_analytics.gold.data_quality_results
(
    run_id STRING,
    rule_name STRING,
    passed_count BIGINT,
    failed_count BIGINT,
    run_timestamp TIMESTAMP
)
USING DELTA;


CREATE TABLE IF NOT EXISTS taxi_analytics.gold.data_quality_summary
(
    rule_name STRING,
    passed_count BIGINT,
    failed_count BIGINT
)
USING DELTA;
