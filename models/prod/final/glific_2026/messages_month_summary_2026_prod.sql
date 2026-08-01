{{ config(
    materialized='table',
    tags=['glific', 'prod', '2026']
) }}

with reporting_months as (
    select *
    from {{ ref('reporting_months_2026_int') }}
),

messages as (
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
),

message_rollup as (
    select
        date_trunc('month', message_at)::date as month_start,
        count(distinct message_id) as total_message_count,
        count(distinct message_id) filter (where flow_name = 'Hi Flow') as hi_flow_message_count,
        count(distinct contact_phone) as users_interacting_count
    from messages
    group by 1
)

select
    rm.month_start,
    rm.month_end,
    rm.reporting_year,
    rm.reporting_year_start,
    rm.reporting_year_end,
    coalesce(mr.total_message_count, 0) as total_message_count,
    coalesce(mr.hi_flow_message_count, 0) as hi_flow_message_count,
    coalesce(mr.users_interacting_count, 0) as users_interacting_count
from reporting_months rm
left join message_rollup mr
  on rm.month_start = mr.month_start
