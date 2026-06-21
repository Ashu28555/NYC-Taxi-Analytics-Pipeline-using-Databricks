CREATE TABLE IF NOT EXISTS taxi_analytics.gold.data_parity_summary
(
    run_id STRING,
    check_name STRING,
    source_value DOUBLE,
    target_value DOUBLE,
    status STRING,
    run_timestamp TIMESTAMP
)
USING DELTA;


CREATE TABLE IF NOT EXISTS taxi_analytics.gold.data_parity_mismatch
(
    run_id STRING,
    check_name STRING,
    source_value DOUBLE,
    target_value DOUBLE,
    difference DOUBLE,
    run_timestamp TIMESTAMP
)
USING DELTA;
