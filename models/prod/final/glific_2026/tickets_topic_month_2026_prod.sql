{{ config(
    materialized='table',
    tags=['glific', 'prod', '2026']
) }}

with tickets as (
    select
        id as ticket_id,
        topic,
        status,
        contact_phone,
        coalesce(
            cast(nullif(bq_inserted_at, '') as timestamptz),
            cast(inserted_at as timestamptz)
        ) as ticket_at
    from {{ source('staging_glific', 'tickets_stg') }}
    where coalesce(
            cast(nullif(bq_inserted_at, '') as timestamptz),
            cast(inserted_at as timestamptz)
          ) >= timestamp '2026-03-01'
      and coalesce(
            cast(nullif(bq_inserted_at, '') as timestamptz),
            cast(inserted_at as timestamptz)
          ) < timestamp '2027-03-01'
      and (
            contact_phone is null
         or contact_phone not like '987654321%'
      )
)

select
    date_trunc('month', ticket_at)::date as month_start,
    (date_trunc('month', ticket_at) + interval '1 month')::date as month_end,
    2026 as reporting_year,
    date '2026-03-01' as reporting_year_start,
    date '2027-03-01' as reporting_year_end,
    coalesce(topic, 'Unknown') as topic,
    count(distinct ticket_id) as ticket_count
from tickets
group by 1, 2, 3, 4, 5, 6
