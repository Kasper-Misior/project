{{ config(
    materialized="ephemeral"
) }}

with source as (
      select * from {{ source('raw_data', 'y_combinator_raw_data') }}
),
renamed as (
    select
        {{ adapter.quote("JSON") }}

    from source
)
select * from renamed
  