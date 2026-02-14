###Marketing Data Warehouse Pipeline
##Overview

This project builds an end-to-end marketing analytics pipeline integrating:

Google Ads

Facebook Ads

CRM Revenue

The pipeline follows a layered architecture:

Python Ingestion (ETL)

PostgreSQL Staging

dbt Transformations (Star Schema)

Airflow Orchestration

Idempotent Loads

Architecture
Source Files (JSON / CSV)
        ↓
Python Ingestion + Data Quality
        ↓
Postgres Staging Tables
        ↓
dbt Models (Dimensions + Facts)
        ↓
Analytics Layer

Ingestion Layer (Python)

Each source has a dedicated script.

CRM – load_crm.py

Process:

Extract CSV with structural validation

Standardize fields (date parsing + channel normalization)

Revenue numeric validation

Reject:

Missing required fields

Invalid channels

Negative revenue

Revenue outliers (IQR method)

Deduplicate by order_id

Add ingested_at (UTC)

Upsert into stg_crm_revenue

Unique constraint: (order_id)

Idempotent using ON CONFLICT

Facebook Ads – load_facebook.py

Process:

Extract campaign CSV

Validate required metrics

Clean numeric fields

Reject invalid or negative values

Add ingestion timestamp

Upsert into stg_facebook_ads

Unique constraint: (campaign_id, date)

Idempotent load

Google Ads – load_google.py

Process:

Extract JSON payload

Flatten daily campaign metrics

Validate metrics + date

Reject invalid records

Add ingestion timestamp

Upsert into stg_google_ads

Unique constraint: (campaign_id, date)

Idempotent load

Data Quality Strategy

Rule-based validation implemented in ingestion:

Required fields enforcement

Type validation

Negative value rejection

Channel validation

Revenue outlier detection using IQR

Deduplication before load

Invalid rows are excluded from staging tables.

Data Model (dbt)
Dimension Tables
dim_channel

channel

dim_campaign

campaign_id

campaign_name

channel

Fact Tables
fct_campaign_performance

activity_date

campaign_id

channel

impressions

clicks

spend

conversions

fct_crm_revenue

order_id

customer_id

activity_date

revenue

channel

campaign_source

product_category

region

Orchestration (Airflow)

DAG: marketing_warehouse_pipeline

Flow:

Ingest CRM
    ↓
Ingest Facebook
    ↓
Ingest Google
    ↓
dbt run
    ↓
dbt test


Features:

Retry logic

Dependency handling

Idempotent ingestion

Manual trigger enabled

LocalExecutor

Idempotency

All ingestion scripts use:

INSERT ... ON CONFLICT (...) DO UPDATE


Re-running the DAG:

Does not create duplicates

Safely updates existing records

Maintains consistent state

Final Outcome

3 integrated marketing sources

Clean staging layer

Star schema dimensional model

Idempotent ingestion

Fully orchestrated pipeline
