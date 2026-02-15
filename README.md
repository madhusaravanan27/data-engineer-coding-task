# Marketing Data Warehouse Pipeline

## Overview

This project builds an end-to-end marketing analytics pipeline integrating:

- Google Ads
- Facebook Ads
- CRM Revenue

The pipeline follows a layered architecture:

- Python Ingestion (ETL)
- PostgreSQL Staging
- dbt Transformations (Star Schema)
- Airflow Orchestration
- Idempotent Loads

---

## Architecture

```text
Source Files (JSON / CSV)
        ↓
Python Ingestion + Data Quality
        ↓
Postgres Staging Tables
        ↓
dbt Models (Dimensions + Facts)
        ↓
Analytics Layer
