{{ config(
    materialized='table',
    tags=['dim']
) }}

with base as (
    select * from {{ ref('stg_organizations') }}
),
enriched as (
    select
        organization_id,
        first_payment_date,
        last_payment_date,
        legal_entity_country_code,
        count_total_contracts_active,
        created_at,
        case when last_payment_date is not null then 1 else 0 end as is_paying_customer,
        datediff('day', first_payment_date, coalesce(last_payment_date, current_date)) as days_since_first_payment
    from base
)
select * from enriched


