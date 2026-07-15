{{ config(
    materialized='table',
    tags=['glific', 'prod', '2026']
) }}

with messages as (
    select
        id as message_id,
        contact_phone,
        flow,
        flow_name,
        coalesce(
            cast(nullif(bq_inserted_at, '') as timestamptz),
            cast(inserted_at as timestamptz)
        ) as message_at
    from {{ source('staging_glific', 'messages_stg') }}
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
      and flow is not null
)

select
    date_trunc('month', message_at)::date as month_start,
    (date_trunc('month', message_at) + interval '1 month')::date as month_end,
    2026 as reporting_year,
    date '2026-03-01' as reporting_year_start,
    date '2027-03-01' as reporting_year_end,
    flow,
    count(distinct message_id) as total_message_count,
    count(distinct message_id) filter (where flow_name = 'Hi Flow') as hi_flow_message_count
from messages
group by 1, 2, 3, 4, 5, 6
