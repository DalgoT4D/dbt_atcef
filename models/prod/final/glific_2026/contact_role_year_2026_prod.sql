{{ config(
    materialized='table',
    tags=['glific', 'prod', '2026']
) }}

with contacts as (
    select *
    from {{ ref('contact_status_year_2026_prod') }}
)

select
    reporting_year,
    reporting_year_start,
    reporting_year_end,
    coalesce(role, 'Unknown') as role,
    count(distinct contact_phone) as contact_count
from contacts
group by 1, 2, 3, 4
