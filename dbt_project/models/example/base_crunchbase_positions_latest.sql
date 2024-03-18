{{ config(
    materialized="ephemeral"
) }}

with source as (
      select * from {{ source('crunchbase', 'positions_latest') }}
),
renamed as (
    select
        {{ adapter.quote("USER_ID") }},
        {{ adapter.quote("COMAPNY_NAME") }},
        {{ adapter.quote("COMPANY_WEBSITE") }},
        {{ adapter.quote("STARTDATE") }},
        {{ adapter.quote("ENDDATE") }},
        {{ adapter.quote("ROLE_K7") }},
        {{ adapter.quote("ROLE_K50") }},
        {{ adapter.quote("CRUNCHBASE_COMPANY_UUID") }}

    from source
)
select * from renamed
  