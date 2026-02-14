{{ config(materialized='table') }}

select distinct
    activity_date as date_day,
    extract(year from activity_date) as year,
    extract(month from activity_date) as month,
    extract(day from activity_date) as day,
    extract(week from activity_date) as week,
    extract(dow from activity_date) as day_of_week
from (
    select activity_date from {{ ref('int_ads_google') }}
    union
    select activity_date from {{ ref('int_ads_facebook') }}
    union
    select activity_date from {{ ref('int_crm_revenue') }}
) d
