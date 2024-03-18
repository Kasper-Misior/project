{{ config(
    materialized="ephemeral"
) }}

with source as (
      select * from {{ source('crunchbase', 'crunchbase_investors') }}
),
renamed as (
    select
        {{ adapter.quote("UUID") }},
        {{ adapter.quote("NAME") }},
        {{ adapter.quote("TYPE") }},
        {{ adapter.quote("PERMALINK") }},
        {{ adapter.quote("CB_URL") }},
        {{ adapter.quote("RANK") }},
        {{ adapter.quote("ROLES") }},
        {{ adapter.quote("DOMAIN") }},
        {{ adapter.quote("COUNTRY_CODE") }},
        {{ adapter.quote("STATE_CODE") }},
        {{ adapter.quote("REGION") }},
        {{ adapter.quote("CITY") }},
        {{ adapter.quote("INVESTOR_TYPES") }},
        {{ adapter.quote("INVESTMENT_COUNT") }},
        {{ adapter.quote("TOTAL_FUNDING_USD") }},
        {{ adapter.quote("TOTAL_FUNDING") }},
        {{ adapter.quote("TOTAL_FUNDING_CURRENCY_CODE") }},
        {{ adapter.quote("FOUNDED_ON") }},
        {{ adapter.quote("CLOSED_ON") }},
        {{ adapter.quote("FACEBOOK_URL") }},
        {{ adapter.quote("LINKEDIN_URL") }},
        {{ adapter.quote("TWITTER_URL") }},
        {{ adapter.quote("LOGO_URL") }}

    from source
)
select * from renamed
  