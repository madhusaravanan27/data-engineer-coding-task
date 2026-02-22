# Marketing Data Warehouse Pipeline

## Overview

This project implements an end-to-end marketing analytics pipeline integrating:

* Google Ads
* Facebook Ads
* CRM Revenue

The pipeline includes ingestion, data validation, staging in PostgreSQL, transformation using dbt, and orchestration via Airflow.

## Tech Stack

* Python
* PostgreSQL
* dbt
* Apache Airflow
* Docker

## Ingestion Layer

Each data source has a dedicated ingestion script:

* load_google.py
* load_facebook.py
* load_crm.py

Each script performs:

1. Data extraction
2. Data quality validation
3. Field standardization
4. Deduplication
5. Timestamp enrichment
6. Idempotent upsert into staging tables

## Data Quality Rules

Validation is applied before loading:

* Required field checks
* Null handling
* Type validation
* Channel validation (google, facebook)
* Negative value rejection
* Revenue outlier detection using IQR (CRM)
* Duplicate prevention before load

Invalid rows are excluded from staging tables.

## Staging Tables

* stg_google_ads
  Unique key: (campaign_id, date)

* stg_facebook_ads
  Unique key: (campaign_id, date)

* stg_crm_revenue
  Unique key: (order_id)

All ingestion scripts use:

```
INSERT ... ON CONFLICT (...) DO UPDATE
```

This ensures idempotent and safe re-runs.


## Data Model (dbt)

### Dimension Tables

* dim_channel
* dim_campaign

### Fact Tables

* fct_campaign_performance
* fct_crm_revenue

The warehouse follows a star schema design to support analytics and reporting.


## Orchestration

Airflow DAG: marketing_warehouse_pipeline

Execution flow:

1. Ingest CRM
2. Ingest Facebook
3. Ingest Google
4. Run dbt models
5. Run dbt tests

The pipeline supports retries and can be safely re-executed.


## How to Run

Start services:

```
docker-compose up -d
```

Access Airflow:

```
http://localhost:8081
```

Run dbt manually (optional):

```
cd dbt/campaignwarehouse
dbt run
dbt test
```

## Key Features

* End-to-end pipeline
* Idempotent ingestion
* Rule-based data validation
* Star schema modeling
* Automated testing
* Fully orchestrated workflow

