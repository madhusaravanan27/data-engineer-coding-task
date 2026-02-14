Marketing Data Warehouse Pipeline
Overview

This project builds a marketing analytics warehouse using:

Python (Ingestion + Data Quality)

PostgreSQL (Staging Layer)

dbt (Transformations)

Airflow (Orchestration)

Docker (Infrastructure)

Setup & Execution Steps
1. Start Infrastructure
docker compose up --build


Services:

PostgreSQL

Airflow (Webserver + Scheduler)

dbt

Adminer

Airflow UI:

http://localhost:8081


Adminer:

http://localhost:8080

2. Ingestion Layer (Python)

Three ingestion scripts:

load_google.py

load_facebook.py

load_crm.py

Each script performs:

Data extraction

Rule-based data quality checks

Outlier handling (IQR for revenue)

Idempotent UPSERT into staging tables

3. Staging Tables (PostgreSQL)
stg_google_ads

campaign_id

campaign_name

campaign_type

date

impressions

clicks

cost

conversions

conversion_value

source_system

ingested_at

Unique Constraint:

(campaign_id, date)

stg_facebook_ads

campaign_id

campaign_name

date

impressions

clicks

spend

purchases

purchase_value

reach

frequency

source_system

ingested_at

Unique Constraint:

(campaign_id, date)

stg_crm_revenue

order_id

customer_id

date

revenue

channel_attributed

campaign_source

product_category

region

ingested_at

Unique Constraint:

(order_id)


All loads use:

ON CONFLICT (...) DO UPDATE


This makes the pipeline idempotent.

4. dbt Transformations

Run:

dbt run
dbt test

Warehouse Models
Dimension Tables
dim_campaign

campaign_id

campaign_name

channel

campaign_type

Fact Tables
fct_campaign_performance

activity_date

campaign_id

channel

impressions

clicks

spend

conversions

revenue

Joins:

Google Ads

Facebook Ads

CRM Revenue

Airflow DAG

DAG Name:

marketing_warehouse_pipeline


Pipeline Order:

ingest_google

ingest_facebook

ingest_crm

dbt_run

dbt_test

Features:

Retry logic

Dependency handling

Safe re-runs (idempotent)

Data Quality Rules

Applied during ingestion:

Required field validation

Channel validation (google / facebook)

Non-negative revenue

IQR-based revenue outlier detection

Duplicate handling via unique constraints