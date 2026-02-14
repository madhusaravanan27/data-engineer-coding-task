select *
from {{ ref('int_crm_revenue') }}
where revenue < 0
