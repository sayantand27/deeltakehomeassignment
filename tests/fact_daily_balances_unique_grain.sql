-- Ensure grain uniqueness: one row per organization_id + date
select organization_id, date, count(*)
from {{ ref('fact_daily_balances') }}
group by 1,2
having count(*) > 1


