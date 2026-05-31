with source as (
    select * from {{ source('warehouse', 'dim_geography') }}
),

filtered as (
    select *
    from source
    where trim(country_code) != 'ZZ'
)

select
    geography_sk,
    trim(country_code)                          as country_code,
    country_name,
    region,
    subregion,
    trim(primary_currency)                      as primary_currency
from filtered
