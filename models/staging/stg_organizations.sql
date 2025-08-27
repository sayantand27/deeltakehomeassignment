{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ ref('organizations') }}
),
typed as (
    select
        cast(organization_id as bigint) as organization_id,
        cast(first_payment_date as date) as first_payment_date,
        cast(last_payment_date as date) as last_payment_date,
        cast(legal_entity_country_code as varchar) as legal_entity_country_code,
        cast(count_total_contracts_active as bigint) as count_total_contracts_active,
        cast(created_at as timestamp) as created_at
    from source
)
select * from typed


