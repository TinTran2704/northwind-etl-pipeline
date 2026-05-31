with source as (
    select * from {{ source('warehouse', 'fact_sales') }}
)

select
    order_id,
    line_number,
    order_date_sk,
    required_date_sk,
    shipped_date_sk,
    customer_sk,
    employee_sk,
    product_sk,
    shipper_sk,
    ship_geography_sk,
    audit_sk,
    quantity,
    unit_price,
    discount,
    extended_price,
    discount_amount,
    net_amount,
    freight_allocated
from source
