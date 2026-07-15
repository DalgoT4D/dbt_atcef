{{ config(
    materialized='table',
    tags=['glific', 'prod', '2026']
) }}

with reporting_months as (
    select *
    from {{ ref('reporting_months_2026_int') }}
),

flow_results as (
    select
        id as flow_result_id,
        name,
        contact_phone,
        coalesce(
            cast(nullif(bq_inserted_at, '') as timestamptz),
            cast(inserted_at as timestamptz)
        ) as flow_result_at
    from {{ source('staging_glific', 'flow_results_stg') }}
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

flow_result_rollup as (
    select
        date_trunc('month', flow_result_at)::date as month_start,
        count(flow_result_id) as flow_result_count,
        count(flow_result_id) filter (where name = 'Hi Flow') as hi_flow_result_count
    from flow_results
    group by 1
)

select
    rm.month_start,
    rm.month_end,
    rm.reporting_year,
    rm.reporting_year_start,
    rm.reporting_year_end,
    coalesce(fr.flow_result_count, 0) as flow_result_count,
    coalesce(fr.hi_flow_result_count, 0) as hi_flow_result_count
from reporting_months rm
left join flow_result_rollup fr
  on rm.month_start = fr.month_start
