{{ config(
    materialized="ephemeral"
) }}

with source as (
      select * from {{ source('crunchbase', 'crunchbase_funding_rounds') }}
),
renamed as (
    select
        {{ adapter.quote("UUID") }},
        {{ adapter.quote("NAME") }},
        {{ adapter.quote("TYPE") }},
        {{ adapter.quote("PERMALINK") }},
        {{ adapter.quote("CB_URL") }},
        {{ adapter.quote("RANK") }},
        {{ adapter.quote("COUNTRY_CODE") }},
        {{ adapter.quote("STATE_CODE") }},
        {{ adapter.quote("REGION") }},
        {{ adapter.quote("CITY") }},
        {{ adapter.quote("INVESTMENT_TYPE") }},
        {{ adapter.quote("ANNOUNCED_ON") }},
        {{ adapter.quote("RAISED_AMOUNT_USD") }},
        {{ adapter.quote("RAISED_AMOUNT") }},
        {{ adapter.quote("RAISED_AMOUNT_CURRENCY_CODE") }},
        {{ adapter.quote("POST_MONEY_VALUATION_USD") }},
        {{ adapter.quote("POST_MONEY_VALUATION") }},
        {{ adapter.quote("POST_MONEY_VALUATION_CURRENCY_CODE") }},
        {{ adapter.quote("INVESTOR_COUNT") }},
        {{ adapter.quote("ORG_UUID") }},
        {{ adapter.quote("ORG_NAME") }},
        {{ adapter.quote("LEAD_INVESTOR_UUIDS") }}

    from source
)
select * from renamed
  