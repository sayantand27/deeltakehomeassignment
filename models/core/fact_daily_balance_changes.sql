{{ config(
    materialized='table',
    tags=['fact']
) }}

with balances as (
    select * from {{ ref('fact_daily_balances') }}
),

with_lag as (
    select
        organization_id,
        date,
        daily_balance,
        lag(daily_balance) over (
            partition by organization_id
            order by date
        ) as prev_balance
    from balances
),

calc as (
    select
        organization_id,
        date,
        daily_balance,
        prev_balance,
        case
            when prev_balance = 0 then null
            else (daily_balance - prev_balance) / nullif(abs(prev_balance), 0)
        end as pct_change
    from with_lag
)

select *
from calc
