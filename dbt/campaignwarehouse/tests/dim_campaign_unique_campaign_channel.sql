select campaign_id, channel, count(*) as cnt
from {{ ref('dim_campaign') }}
group by 1,2
having count(*) > 1
