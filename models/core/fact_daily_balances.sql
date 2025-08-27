{{ config(
    materialized='table',
    tags=['fact']
) }}

with dates as (
    select distinct txn_date as date from {{ ref('stg_invoices') }}
),
orgs as (
    select organization_id from {{ ref('dim_organizations') }}
),
calendar as (
    select o.organization_id, d.date
    from orgs o
    cross join dates d
),
norm as (
    -- Normalize to USD directly using amount_usd from staging
    select
      organization_id,
      txn_date as date,
      status,
      amount_usd
    from {{ ref('stg_invoices') }}
),
signed as (
    -- Apply sign based on status: paid reduces balance (-), credited increases balance (+);
    -- treat refunded as positive (money out to customer reduces our balance? adjust per definition).
    select
      organization_id,
      date,
      case
        when lower(status) in ('paid','refund','refunded') then -1
        when lower(status) in ('credited') then 1
        when lower(status) in ('open','pending','awaiting_payment','processing','failed','skipped','unpayable','cancelled','canceled') then 0
        else 0
      end * amount_usd as signed_amount
    from norm
),
txns as (
    select
        organization_id,
        txn_date as date,
        sum(signed_amount) as daily_amount
    from signed
    group by 1,2
),
accum as (
    select
        c.organization_id,
        c.date,
        coalesce(t.daily_amount, 0.0) as daily_amount,
        sum(coalesce(t.daily_amount, 0.0)) over (
            partition by c.organization_id order by c.date
            rows between unbounded preceding and current row
        ) as daily_balance
    from calendar c
    left join txns t on t.organization_id = c.organization_id and t.date = c.date
)
select organization_id, date, daily_balance from accum


