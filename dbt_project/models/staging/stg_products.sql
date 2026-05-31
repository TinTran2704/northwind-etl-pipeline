with source as (
    select * from {{ source('warehouse', 'dim_product') }}
),

filtered as (
    select *
    from source
    where product_sk != -1
      and is_current = true
)

select
    product_sk,
    product_nk                                  as product_code,
    product_name,
    category_name,
    supplier_name,
    trim(supplier_country)                      as supplier_country_code,
    quantity_per_unit,
    unit_price,
    units_in_stock,
    discontinued,
    effective_date,
    expiration_date,
    is_current,
    audit_sk
from filtered
