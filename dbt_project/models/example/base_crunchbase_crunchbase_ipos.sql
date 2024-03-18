{{ config(
    materialized="ephemeral"
) }}

with source as (
      select * from {{ source('crunchbase', 'crunchbase_ipos') }}
),
renamed as (
    select
        {{ adapter.quote("UUID") }},
        {{ adapter.quote("NAME") }},
        {{ adapter.quote("TYPE") }},
        {{ adapter.quote("PERMALINK") }},
        {{ adapter.quote("CB_URL") }},
        {{ adapter.quote("RANK") }},
        {{ adapter.quote("ORG_UUID") }},
        {{ adapter.quote("ORG_NAME") }},
        {{ adapter.quote("ORG_CB_URL") }},
        {{ adapter.quote("COUNTRY_CODE") }},
        {{ adapter.quote("STATE_CODE") }},
        {{ adapter.quote("REGION") }},
        {{ adapter.quote("CITY") }},
        {{ adapter.quote("STOCK_EXCHANGE_SYMBOL") }},
        {{ adapter.quote("STOCK_SYMBOL") }},
        {{ adapter.quote("WENT_PUBLIC_ON") }},
        {{ adapter.quote("SHARE_PRICE_USD") }},
        {{ adapter.quote("SHARE_PRICE") }},
        {{ adapter.quote("SHARE_PRICE_CURRENCY_CODE") }},
        {{ adapter.quote("VALUATION_PRICE_USD") }},
        {{ adapter.quote("VALUATION_PRICE") }},
        {{ adapter.quote("VALUATION_PRICE_CURRENCY_CODE") }},
        {{ adapter.quote("MONEY_RAISED_USD") }},
        {{ adapter.quote("MONEY_RAISED") }},
        {{ adapter.quote("MONEY_RAISED_CURRENCY_CODE") }}

    from source
)
select * from renamed
  