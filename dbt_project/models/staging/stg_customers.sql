with source as (
    select * from {{ source('warehouse', 'dim_customer') }}
),

filtered as (
    select *
    from source
    where customer_sk != -1
      and is_current = true
)

select
    customer_sk,
    customer_nk                                 as customer_code,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    postal_code,
    trim(country_code)                          as country_code,
    region_name,
    phone,
    effective_date,
    expiration_date,
    is_current,
    audit_sk
from filtered
