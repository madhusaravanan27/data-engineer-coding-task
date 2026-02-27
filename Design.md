# DESIGN.md — Marketing Warehouse Pipeline

## Overview

This repository implements a local marketing analytics warehouse pipeline that ingests:
- Google Ads daily campaign metrics (JSON payload)
- Facebook Ads daily campaign metrics (CSV export)
- CRM order/revenue data (CSV export with potential structural issues)

The system loads each source into Postgres staging tables and uses dbt to standardize, test, and build analytics-ready marts.

High-level flow:
1) **Extract** (files simulating APIs/exports)
2) **Validate / DQ** (row-level and structural rules)
3) **Transform** (type normalization, canonical metrics, lineage columns)
4) **Load** into Postgres staging (idempotent upsert)
5) **dbt run** to build intermediate + marts
6) **dbt test** as a quality gate

Orchestration is handled via Airflow DAG. Runtime environment is docker-compose.


## System Architecture

### Components
- **Postgres**: warehouse + staging tables; also Airflow metadata DB in this local setup
- **Airflow**: orchestrates ingestion scripts + dbt commands
- **dbt**: transforms staging data into standardized intermediates and marts; runs tests
- **Adminer**: visual DB inspection
- **Jupyter**: optional exploration/debugging

### Data Flow
- Ingestion scripts populate:
  - `stg_google_ads`
  - `stg_facebook_ads`
  - `stg_crm_revenue`
- dbt reads from staging sources, builds:
  - `int_ads_google`, `int_ads_facebook` (standardized ad platform shapes)
  - `fct_ads_performance_daily` (unified paid performance)
  - `fct_crm_campaign_daily` (CRM aggregated to campaign-day)
  - dimensions like `dim_campaign`, `dim_channel`
- Final unified marts can join ads + CRM at consistent grain.


## Schema Design Decisions and Rationale

### Staging Tables (Postgres)
**Design principle**: staging tables are source-aligned but cleaned enough to be reliable inputs to dbt.

Common columns added:
- `source_system` (lineage tag: google/facebook/crm)
- `ingested_at` (UTC processing timestamp)

#### `stg_google_ads`
- Grain: **(campaign_id, date)** (one row per campaign per day)
- Rationale: daily campaign metrics are naturally aggregated at day level
- Constraint: UNIQUE(campaign_id, date) to enforce grain

#### `stg_facebook_ads`
- Grain: **(campaign_id, date)**
- Similar constraint + rationale as Google
- Additional metrics: reach, frequency (platform-specific but preserved)

#### `stg_crm_revenue`
- Grain: **order_id** (one row per order)
- Rationale: CRM is transactional truth; order_id is the natural business key
- Constraint: UNIQUE(order_id) to enforce idempotent upsert

## dbt Model Layering and Rationale

### Intermediate Models
Goal: standardize source differences to a consistent schema and enable safe joins/aggregations.

- `int_ads_google`, `int_ads_facebook`:
  - Standard columns: `activity_date`, `channel`, `campaign_id`, `campaign_name`, `impressions`, `clicks`, `spend`, `conversions`, `conversion_value`
  - Rationale: makes downstream unioning possible with consistent semantics

- `int_crm_revenue`:
  - Keeps CRM at order grain; normalizes `channel` and maps `campaign_source -> campaign_id`
  - Rationale: preserves transactional detail and avoids premature aggregation

### Fact Tables / Marts
- `fct_ads_performance_daily`:
  - Built via `UNION ALL` of standardized ad platform models and aggregated to:
    - Grain: (activity_date, channel, campaign_id)
  - Rationale: one unified paid media performance fact for BI

- `fct_crm_campaign_daily`:
  - Aggregates CRM to campaign-day:
    - Grain: (activity_date, channel, campaign_id)
    - Metrics: `orders`, `revenue`
  - Rationale: aligns CRM outcomes to same grain as ads fact to prevent row explosion and double counting

### Dimensions
- `dim_campaign`:
  - Canonical campaign lookup by (campaign_id, channel)
  - Note: current implementation selects `max(campaign_name)`; with more time we’d choose the latest name by date/ingested_at
- `dim_channel`:
  - Distinct list of channels from ads + CRM


## Data Quality Strategy

### Philosophy
DQ checks are applied in two places:
1) **Ingestion-time guardrails**: prevent obviously broken or structurally corrupted data from entering staging
2) **dbt tests**: enforce constraints in the transformation layer, catching drift and regressions

### Structural Validation (CRM CSV)
CRM ingestion performs structural validation using Python `csv.reader`:
- Ensures each row has the same number of fields as the header
- Rationale: prevents silent column shifting due to malformed CSV lines (unescaped commas, corrupted rows)
- Action: structurally bad rows are excluded from loading (logged for visibility)

### Row-level DQ Rules (Ingestion)
**Google / Facebook**
- Required fields non-null: campaign_id, date, key metrics
- Non-negative metrics: impressions, clicks, cost/spend, conversions/value
- Duplicate key detection: (campaign_id, date)
- Action: reject invalid rows before staging load

**CRM**
- Required fields non-null: order_id, customer_id, parsed date, revenue, channel
- Channel whitelist: {google, facebook} for attributed paid channels (local demo scope)
- Revenue coercion to numeric; invalid values become null and rejected
- Negative revenue rejected in this implementation (see ADR below)
- Outliers flagged by IQR bounds and rejected in this implementation (see ADR below)
- Deduplication: keep last record per order_id after sorting by date

### dbt Testing
dbt tests are executed after model build:
- not_null for primary keys
- unique for business keys at expected grain
- accepted_values for channel where applicable
- (optional) relationships tests for joins if dimensions are introduced


## Trade-offs Made Given Time Constraints

### 1) Dropping negatives and outliers vs quarantining
- Implemented: filter out negative revenue and IQR outliers during ingestion
- Trade-off: protects dashboards from extreme skew, but risks excluding legitimate refunds or high-value orders
- Time constraint: quarantine + review workflow and alerting was not implemented

### 2) Airflow metadata DB sharing Postgres with warehouse
- Implemented: one Postgres instance
- Trade-off: simpler local setup, but mixes workloads and is not ideal for production isolation

### 3) dbt inside Airflow container (no separate dbt service)
- Implemented: install dbt-postgres in the Airflow image and run via BashOperator
- Trade-off: tighter coupling but simplest orchestration for a take-home / local stack

### 4) Campaign name canonicalization using `max()`
- Implemented: `max(campaign_name)` per campaign_id + channel
- Trade-off: not a guaranteed “latest correct” name if names change over time


## What I’d Do Differently With More Time

1) **Quarantine instead of drop**
   - Store rejected rows in `dq_rejects_*` tables with `_dq_reason`, source file, and ingestion timestamp
   - Alert when rejection rate exceeds threshold

2) **Model refunds/chargebacks explicitly**
   - Keep negative revenue as a separate transaction type or dedicated fact table
   - Provide both gross and net revenue marts

3) **More robust outlier handling**
   - Flag outliers and route to quarantine
   - Use domain rules (currency unit checks, max expected order size) rather than only IQR
   - Allow high-value orders with manual/automated validation

4) **Better dimension modeling**
   - Implement “latest campaign_name wins” using activity_date/ingested_at
   - Add surrogate keys if needed for BI performance

5) **Incremental dbt builds**
   - Convert heavy marts to incremental materializations if volume grows

6) **Separation of concerns**
   - Separate Airflow metadata DB from analytics warehouse schema/DB
   - Add secrets management rather than inline passwords

# Architecture Decision Records (ADRs)

## ADR-001: Orchestration Tool Choice

**Decision**: Use Airflow to orchestrate ingestion + dbt run/test.

**Options Considered**
1) Airflow DAG (chosen)
2) Cron + bash scripts
3) dbt Cloud jobs only
4) Prefect / Dagster

**Why This Approach**
- Airflow provides clear task dependency management, retries, and observability
- Standard tool in DE stacks and commonly used to orchestrate dbt runs
- Fits the multi-step workflow (ingest -> dbt run -> dbt test)

**Trade-offs**
- More setup complexity than cron for a small pipeline
- LocalExecutor is not horizontally scalable (acceptable for local demo)


## ADR-002: CRM Structural Validation Strategy

**Decision**: Validate CSV structure using `csv.reader` before pandas.

**Options Considered**
1) pandas `read_csv()` with error handling
2) Python csv module structural gate (chosen)
3) Enforce schema via Great Expectations / Pandera

**Why This Approach**
- Structural issues (wrong field count) can silently shift columns in pandas
- csv-based validation lets us identify malformed lines and avoid corrupt staging loads

**Trade-offs**
- Bad rows are currently dropped rather than quarantined
- Adds code complexity compared to relying on pandas


## ADR-003: Revenue Negative Value Handling

**Decision**: Exclude negative revenue rows from analytics staging and marts.

**Options Considered**
1) Keep negative values as-is
2) Exclude negative values (chosen)
3) Model negative values as refunds/chargebacks in a separate fact

**Why This Approach**
- This take-home focuses on marketing performance; negative revenue usually reflects refunds/adjustments that require different business logic and timing
- Prevents campaign-day revenue from being distorted in basic ROAS reporting

**Trade-offs**
- Risk of excluding legitimate refund events that are business-relevant
- With more time, we would store negatives separately and produce gross vs net marts


## ADR-004: Outlier Treatment in CRM Revenue

**Decision**: Identify outliers using IQR and exclude from load into analytics tables.

**Options Considered**
1) Keep all values and rely on downstream handling
2) IQR-based outlier filtering (chosen)
3) Quarantine outliers and alert (preferred with more time)
4) Domain-based thresholds (e.g., max order value by product category)

**Why This Approach**
- Extreme anomalies can dominate aggregates (ROAS, revenue) and mislead analysis
- IQR provides a simple statistical guardrail for a local demo

**Trade-offs**
- Outliers might be legitimate high-value orders
- Better approach is quarantine + review and/or domain-specific validation


## ADR-005: dbt Materialization Choices

**Decision**: Use `table` for aggregated marts, views for light intermediates.

**Options Considered**
1) Views everywhere
2) Tables everywhere
3) Mixed strategy (chosen)

**Why This Approach**
- Aggregations are expensive; table materialization avoids recomputation and speeds BI queries
- Intermediates can be views to keep transformation logic centralized and transparent

**Trade-offs**
- Tables require rebuild/refresh; without incremental, full rebuild happens each run (acceptable for local scale)


## ADR-006: Idempotent Loads via Postgres UPSERT

**Decision**: Use `ON CONFLICT DO UPDATE` to upsert into staging.

**Options Considered**
1) Delete + insert each run
2) Append-only loads
3) Upsert on business keys (chosen)

**Why This Approach**
- Airflow retries and reruns must not create duplicates
- Marketing APIs and exports may revise metrics; upsert supports corrections

**Trade-offs**
- Upserts can be slower than bulk append for huge datasets
- For scale, we’d consider partitioning, COPY-based loads, or CDC patterns


## Appendix: Key Grains (to prevent analytics corruption)

- Ads platform facts: (campaign_id, channel, activity_date)
- CRM raw fact: (order_id)
- CRM aggregated for marketing: (campaign_id, channel, activity_date)

All joins between ads and CRM are performed only after aligning grain to avoid metric inflation.
