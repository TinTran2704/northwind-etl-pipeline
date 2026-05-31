with source as (
    select * from {{ source('warehouse', 'dim_shipper') }}
),

filtered as (
    select *
    from source
    where shipper_sk != -1
)

select
    shipper_sk,
    shipper_nk                                  as shipper_code,
    company_name,
    phone,
    audit_sk
from filtered
