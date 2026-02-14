{{ config(materialized='table') }}

with campaigns as (

    select
        campaign_id,
        channel,
        campaign_name
    from {{ ref('int_ads_google') }}
    where campaign_id is not null

    union all

    select
        campaign_id,
        channel,
        campaign_name
    from {{ ref('int_ads_facebook') }}
    where campaign_id is not null

)

select
    campaign_id,
    channel,
    max(campaign_name) as campaign_name
from campaigns
group by campaign_id, channel
