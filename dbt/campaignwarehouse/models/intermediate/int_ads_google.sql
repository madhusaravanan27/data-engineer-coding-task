{{ config(materialized='view') }}

select
    date::date as activity_date,
    'google' as channel,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    cost as spend,
    conversions,
    conversion_value
from {{ source('staging_input', 'stg_google_ads') }}
