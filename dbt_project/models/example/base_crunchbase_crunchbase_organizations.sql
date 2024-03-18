{{ config(
    materialized="ephemeral"
) }}

with source as (
      select * from {{ source('crunchbase', 'crunchbase_organizations') }}
),
renamed as (
    select
        {{ adapter.quote("UUID") }},
        {{ adapter.quote("NAME") }},
        {{ adapter.quote("TYPE") }},
        {{ adapter.quote("PERMALINK") }},
        {{ adapter.quote("CB_URL") }},
        {{ adapter.quote("RANK") }},
        {{ adapter.quote("LEGAL_NAME") }},
        {{ adapter.quote("ROLES") }},
        {{ adapter.quote("DOMAIN") }},
        {{ adapter.quote("HOMEPAGE_URL") }},
        {{ adapter.quote("COUNTRY_CODE") }},
        {{ adapter.quote("STATE_CODE") }},
        {{ adapter.quote("REGION") }},
        {{ adapter.quote("CITY") }},
        {{ adapter.quote("ADDRESS") }},
        {{ adapter.quote("POSTAL_CODE") }},
        {{ adapter.quote("STATUS") }},
        {{ adapter.quote("SHORT_DESCRIPTION") }},
        {{ adapter.quote("CATEGORY_LIST") }},
        {{ adapter.quote("CATEGORY_GROUPS_LIST") }},
        {{ adapter.quote("NUM_FUNDING_ROUNDS") }},
        {{ adapter.quote("TOTAL_FUNDING_USD") }},
        {{ adapter.quote("TOTAL_FUNDING") }},
        {{ adapter.quote("TOTAL_FUNDING_CURRENCY_CODE") }},
        {{ adapter.quote("FOUNDED_ON") }},
        {{ adapter.quote("LAST_FUNDING_ON") }},
        {{ adapter.quote("CLOSED_ON") }},
        {{ adapter.quote("EMPLOYEE_COUNT") }},
        {{ adapter.quote("EMAIL") }},
        {{ adapter.quote("PHONE") }},
        {{ adapter.quote("FACEBOOK_URL") }},
        {{ adapter.quote("LINKEDIN_URL") }},
        {{ adapter.quote("TWITTER_URL") }},
        {{ adapter.quote("LOGO_URL") }},
        {{ adapter.quote("ALIAS1") }},
        {{ adapter.quote("ALIAS2") }},
        {{ adapter.quote("ALIAS3") }},
        {{ adapter.quote("PRIMARY_ROLE") }},
        {{ adapter.quote("NUM_EXITS") }}

    from source
)
select * from renamed
  