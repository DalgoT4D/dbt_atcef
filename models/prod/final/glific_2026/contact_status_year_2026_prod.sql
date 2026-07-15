{{ config(
    materialized='table',
    tags=['glific', 'prod', '2026']
) }}

with contacts as (
    select
        c.id as contact_id,
        c.phone as contact_phone,
        c.inserted_date as contact_inserted_date,
        c.updated_date as contact_updated_date,
        coalesce(nullif(cff."Role", ''), 'Unknown') as role,
        cff.username_lms,
        split_part(nullif(cff.username_lms, ''), '@', 2) as project
    from {{ ref('contacts_int') }} c
    left join {{ ref('contacts_field_flatten_int') }} cff
      on c.id = cff.contact_id
    where c.phone is not null
      and c.phone not like '987654321%'
      and c.inserted_date < date '2027-03-01'
),

message_rollup as (
    select
        m.contact_phone,
        count(distinct m.id) filter (where m.flow = 'inbound') as inbound_message_count,
        count(distinct m.id) filter (where m.flow_name = 'Hi Flow') as hi_flow_message_count
    from {{ source('staging_glific', 'messages_stg') }} m
    where coalesce(
            cast(nullif(m.bq_inserted_at, '') as timestamptz),
            cast(m.inserted_at as timestamptz)
          ) >= timestamp '2026-03-01'
      and coalesce(
            cast(nullif(m.bq_inserted_at, '') as timestamptz),
            cast(m.inserted_at as timestamptz)
          ) < timestamp '2027-03-01'
      and m.contact_phone is not null
      and m.contact_phone not like '987654321%'
    group by 1
)

select
    2026 as reporting_year,
    date '2026-03-01' as reporting_year_start,
    date '2027-03-01' as reporting_year_end,
    c.contact_id,
    c.contact_phone,
    c.contact_inserted_date,
    c.contact_updated_date,
    c.role,
    c.username_lms,
    c.project,
    coalesce(mr.inbound_message_count, 0) as inbound_message_count,
    coalesce(mr.hi_flow_message_count, 0) as hi_flow_message_count,
    case
        when coalesce(mr.inbound_message_count, 0) >= 1
         and coalesce(mr.hi_flow_message_count, 0) >= 1 then 'Active'
        when coalesce(mr.inbound_message_count, 0) >= 1
         and coalesce(mr.hi_flow_message_count, 0) = 0 then 'Dormant'
        else 'Inactive'
    end as user_status
from contacts c
left join message_rollup mr
  on c.contact_phone = mr.contact_phone
