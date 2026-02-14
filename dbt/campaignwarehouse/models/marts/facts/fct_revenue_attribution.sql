{{ config(materialized='table') }}

select
    activity_date,
    channel,
    campaign_id,
    count(order_id) as orders,
    sum(revenue) as revenue

from {{ ref('int_crm_revenue') }}
group by 1,2,3
