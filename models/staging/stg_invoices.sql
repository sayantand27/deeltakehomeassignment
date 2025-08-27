{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ ref('invoices') }}
),
typed as (
    select
        cast(INVOICE_ID as number) as invoice_id,
        cast(PARENT_INVOICE_ID as number) as parent_invoice_id,
        cast(TRANSACTION_ID as number) as transaction_id,
        cast(ORGANIZATION_ID as number) as organization_id,
        cast(TYPE as number) as type_code,
        cast(STATUS as varchar) as status,
        cast(CURRENCY as varchar) as currency,
        cast(PAYMENT_CURRENCY as varchar) as payment_currency,
        cast(PAYMENT_METHOD as varchar) as payment_method,
        cast(AMOUNT as float) as amount,
        cast(PAYMENT_AMOUNT as float) as payment_amount,
        cast(FX_RATE as float) as fx_rate,
        cast(FX_RATE_PAYMENT as float) as fx_rate_payment,
        cast(CREATED_AT as timestamp_tz) as created_at,
        to_date(CREATED_AT) as txn_date,
        -- Normalize to USD using CURRENCY and FX_RATE (assume FX_RATE = units of currency per 1 USD)
        case
            when upper(CURRENCY) = 'USD' then cast(AMOUNT as float)
            when FX_RATE is not null and FX_RATE != 0 then cast(AMOUNT as float) / cast(FX_RATE as float)
            else null
        end as amount_usd
    from source
)
select * from typed


