CREATE SCHEMA IF NOT EXISTS taxi_analytics.metadata;

CREATE TABLE IF NOT EXISTS taxi_analytics.metadata.pipeline_audit
(
    run_id STRING,
    pipeline_name STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    rows_processed BIGINT,
    status STRING
)
USING DELTA;


CREATE TABLE IF NOT EXISTS taxi_analytics.metadata.rule_master
(
    rule_id INT,
    rule_name STRING,
    rule_description STRING
)
USING DELTA;


INSERT INTO taxi_analytics.metadata.rule_master VALUES
(1,'Positive Fare','fare_amount > 0');

INSERT INTO taxi_analytics.metadata.rule_master VALUES
(2,'Positive Distance','trip_distance > 0');

INSERT INTO taxi_analytics.metadata.rule_master VALUES
(3,'Valid Passenger Count','passenger_count > 0');

INSERT INTO taxi_analytics.metadata.rule_master VALUES
(4,'Valid Duration','trip_duration_minutes <= 180');

INSERT INTO taxi_analytics.metadata.rule_master VALUES
(5,'Pickup Not Null','pickup datetime present');
