{{ config(materialized='table') }}

select
    activity_date,
    channel,
    campaign_id,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(spend) as spend,
    sum(conversions) as conversions,
    sum(conversion_value) as conversion_value

    
from (
    select * from {{ ref('int_ads_google') }}
    union all
    select * from {{ ref('int_ads_facebook') }}
) a
group by 1,2,3
