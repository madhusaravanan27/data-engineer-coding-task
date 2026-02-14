{{ config(materialized='table') }}

select distinct
    channel
from (
    select channel from {{ ref('int_ads_google') }}
    union
    select channel from {{ ref('int_ads_facebook') }}
    union
    select channel from {{ ref('int_crm_revenue') }}
) c
