# Marketing Data Warehouse Pipeline

## Overview

This project implements an end-to-end marketing data pipeline using:

- Python (ingestion + data quality)
- Postgres (data warehouse)
- dbt (transformations)
- Airflow (orchestration)
- Docker (reproducibility)

The pipeline ingests Google Ads, Facebook Ads, and CRM Revenue data, applies rule-based data validation, loads staging tables, and builds a dimensional model for analytics.

---

## Architecture

Source Files / API  
↓  
Python Ingestion (Extract → Validate → Transform → Upsert)  
↓  
Postgres Staging Tables  
↓  
dbt Transformations  
↓  
Fact & Dimension Tables  
↓  
Airflow DAG Orchestration  

---

## Ingestion Layer (Python)

Each source has a dedicated script:

