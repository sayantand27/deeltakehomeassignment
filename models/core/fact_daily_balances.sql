{{ config(
    materialized='table',
    tags=['fact']
) }}

-- Step 1: Get all relevant dates from invoices
with dates as (
    select distinct txn_date as date
    from {{ ref('stg_invoices') }}
),

-- Step 2: Get all organizations from dimension table
orgs as (
    select organization_id
    from {{ ref('dim_organizations') }}
),

-- Step 3: Build full date × organization calendar
calendar as (
    select o.organization_id, d.date
    from orgs o
    cross join dates d
),

-- Step 4: Normalize invoice data (only USD)
norm as (
    select
        organization_id,
        txn_date as date,
        lower(status) as status,
        amount_usd
    from {{ ref('stg_invoices') }}
),

-- Step 5: Assign signed amount based on status
signed as (
    select
        organization_id,
        date,
        case
            when status in ('paid', 'refund', 'refunded') then -1
            when status = 'credited' then 1
            else 0
        end * amount_usd as signed_amount
    from norm
),

-- Step 6: Sum signed amounts per org per day
txns as (
    select
        organization_id,
        date,
        sum(signed_amount) as daily_amount
    from signed
    group by organization_id, date
),

-- Step 7: Compute running balance per org
accum as (
    select
        c.organization_id,
        c.date,
        coalesce(t.daily_amount, 0.0) as daily_amount,
        sum(coalesce(t.daily_amount, 0.0)) over (
            partition by c.organization_id
            order by c.date
            rows between unbounded preceding and current row
        ) as daily_balance
    from calendar c
    left join txns t
        on t.organization_id = c.organization_id
        and t.date = c.date
)

-- Final select
select
    organization_id,
    date,
    daily_balance,
    daily_amount  
from accum
