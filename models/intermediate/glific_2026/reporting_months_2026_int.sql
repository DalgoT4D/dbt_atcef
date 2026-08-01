{{ config(
    materialized='table',
    tags=['glific', 'intermediate', '2026']
) }}

with months as (
    select
        generate_series(
            date '2026-03-01',
            date '2027-02-01',
            interval '1 month'
        )::date as month_start
)

select
    month_start,
    (month_start + interval '1 month')::date as month_end,
    2026 as reporting_year,
    date '2026-03-01' as reporting_year_start,
    date '2027-03-01' as reporting_year_end
from months
