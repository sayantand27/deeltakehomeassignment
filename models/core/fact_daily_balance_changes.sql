{{ config(
    materialized='table',
    tags=['fact']
) }}

with balances as (
    select * from {{ ref('fact_daily_balances') }}
),
calc as (
    select
        organization_id,
        date,
        daily_balance,
        lag(daily_balance) over (partition by organization_id order by date) as prev_balance,
        case when lag(daily_balance) over (partition by organization_id order by date) = 0 then null
             else (daily_balance - lag(daily_balance) over (partition by organization_id order by date))
                  / nullif(lag(daily_balance) over (partition by organization_id order by date), 0)
        end as pct_change
    from balances
)
select * from calc


