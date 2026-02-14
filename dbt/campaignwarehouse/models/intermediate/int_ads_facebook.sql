{{ config(materialized='view') }}

select
    date::date as activity_date,
    'facebook' as channel,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    spend,
    purchases as conversions,
    purchase_value as conversion_value
from {{ source('staging_input', 'stg_facebook_ads') }}
