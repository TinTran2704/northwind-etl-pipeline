with source as (
    select * from {{ source('warehouse', 'dim_date') }}
),

filtered as (
    select *
    from source
    where date_sk != 19000101
)

select
    date_sk,
    full_date,
    day_of_week,
    day_name,
    day_of_month,
    day_of_year,
    week_of_year,
    month,
    month_name,
    quarter,
    year,
    is_weekend,
    fiscal_year,
    fiscal_quarter
from filtered
