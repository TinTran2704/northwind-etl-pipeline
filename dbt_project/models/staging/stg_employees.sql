with source as (
    select * from {{ source('warehouse', 'dim_employee') }}
),

filtered as (
    select *
    from source
    where employee_sk != -1
      and is_current = true
)

select
    employee_sk,
    employee_nk                                 as employee_code,
    full_name,
    title,
    reports_to_nk                               as reports_to_code,
    hire_date,
    city,
    trim(country_code)                          as country_code,
    effective_date,
    expiration_date,
    is_current,
    audit_sk
from filtered
