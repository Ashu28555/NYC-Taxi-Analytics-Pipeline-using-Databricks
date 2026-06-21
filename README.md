# NYC Taxi Analytics Pipeline using Databricks

End-to-end Data Engineering project built on Databricks Community Edition using Medallion Architecture (Bronze → Silver → Gold), Data Quality Validation, Data Parity Checks, Star Schema Modeling, and Analytics Mart creation.

---

# Technologies Used

- Databricks Community Edition
- PySpark
- Delta Lake
- Unity Catalog
- Workflow Jobs
- Star Schema Modeling
- Data Quality Framework

---

# Architecture

Raw Files
        ↓
Bronze Layer
        ↓
Silver Layer
(Data Validation)
        ↓
Data Parity Check
        ↓
Gold Star Schema
        ↓
Analytics Mart

---

# Project Modules

## 1. Bronze Layer

Notebook:

```
01_bronze_ingestion.py
```

Functions:

- Load parquet files
- Load zone lookup data
- Generate surrogate keys
- Enable Change Data Feed
- Maintain audit logs

---

## 2. Silver Layer

Notebook:

```
02_silver_transform_validation.py
```

Features:

- Trip duration calculation
- Data quality rules
- Valid and invalid record separation
- Failed records table
- DQ metrics logging

---

## 3. Data Parity Validation

Notebook:

```
03_data_parity_check.py
```

Checks:

- Row count validation
- Revenue parity
- Trip distance parity

Outputs:

- data_parity_summary
- data_parity_mismatch

---

## 4. Gold Star Schema

Notebook:

```
04_gold_star_schema.py
```

Dimensions:

- Dim Date
- Dim Zone
- Dim Payment

Fact:

- Fact Trips

---

## 5. Analytics Mart

Notebook:

```
05_analytics_mart.py
```

Aggregations:

- Trip Summary by Day
- Revenue by Borough
- Payment Analysis
- Top Pickup Zones
- Data Quality Summary

---

# Workflow Orchestration

Implemented using Databricks Jobs.

Pipeline Sequence:

1. Bronze Ingestion
2. Silver Transformation
3. Data Parity Check
4. Gold Star Schema
5. Analytics Mart

---

# Metadata Tables

- pipeline_audit
- rule_master

---

# Gold Tables

### Fact Table

- fact_trips

### Dimension Tables

- dim_date
- dim_zone
- dim_payment_type

### Analytical Tables

- trip_summary_by_day
- revenue_by_borough
- payment_analysis
- top_pickup_zones
- data_quality_summary

---

# Future Enhancements

- Incremental Load using Change Data Feed
- SCD Type 2 Dimensions
- Power BI Dashboard
- CI/CD using GitHub Actions
- dbt Integration

---
