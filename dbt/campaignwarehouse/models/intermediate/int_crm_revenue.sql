{{ config(materialized='view') }}

select
    order_id,
    date::date as activity_date,
    lower(channel_attributed) as channel,
    campaign_source as campaign_id,
    revenue,
    region,
    product_category,
    1 as purchases
from {{ source('staging_input', 'stg_crm_revenue') }}
where revenue is not null
  and revenue >= 0
