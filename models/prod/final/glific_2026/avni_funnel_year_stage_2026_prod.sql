{{ config(
    materialized='table',
    tags=['glific', 'prod', '2026']
) }}

with contact_base as (
    select
        c.id as contact_id,
        c.phone as contact_phone,
        coalesce(nullif(cff."Role", ''), 'Unknown') as role,
        cff.username_lms,
        split_part(nullif(cff.username_lms, ''), '@', 2) as project
    from {{ ref('contacts_int') }} c
    left join {{ ref('contacts_field_flatten_int') }} cff
      on c.id = cff.contact_id
    where c.phone is not null
      and c.phone not like '987654321%'
),

module_events as (
    select
        contact_id,
        phone as contact_phone,
        field_inserted_at,
        field_value::int as module_no
    from {{ ref('contact_raw_field_events_2026_int') }}
    where field_name = 'module_no'
      and field_inserted_at >= timestamp '2026-03-01'
      and field_inserted_at < timestamp '2027-03-01'
      and field_value ~ '^[0-9]+$'
),

latest_module as (
    select *
    from (
        select
            contact_id,
            contact_phone,
            field_inserted_at,
            module_no,
            row_number() over (
                partition by contact_id
                order by field_inserted_at desc nulls last, module_no desc
            ) as rn
        from module_events
    ) ranked
    where rn = 1
),

access_events as (
    select
        contact_id,
        max(case when lower(field_value) = 'yes' then 1 else 0 end) as avni_training_accessed
    from {{ ref('contact_raw_field_events_2026_int') }}
    where field_name = 'avni_trainining_accessed'
      and field_inserted_at >= timestamp '2026-03-01'
      and field_inserted_at < timestamp '2027-03-01'
    group by 1
),

done_events as (
    select
        contact_id,
        max(case when lower(field_value) = 'y' then 1 else 0 end) as avni_training_done
    from {{ ref('contact_raw_field_events_2026_int') }}
    where field_name = 'is_avni_training_done'
      and field_inserted_at >= timestamp '2026-03-01'
      and field_inserted_at < timestamp '2027-03-01'
    group by 1
),

event_contacts as (
    select contact_id from latest_module
    union
    select contact_id from access_events
    union
    select contact_id from done_events
),

progress as (
    select
        ec.contact_id,
        cb.contact_phone,
        cb.role,
        cb.username_lms,
        cb.project,
        coalesce(ae.avni_training_accessed, 0) as avni_training_accessed,
        lm.module_no as latest_module_no,
        coalesce(de.avni_training_done, 0) as avni_training_done,
        case when lm.module_no is not null then 1 else 0 end as started_avni_training
    from event_contacts ec
    join contact_base cb
      on ec.contact_id = cb.contact_id
    left join latest_module lm
      on ec.contact_id = lm.contact_id
    left join access_events ae
      on ec.contact_id = ae.contact_id
    left join done_events de
      on ec.contact_id = de.contact_id
),

module_numbers as (
    select generate_series(
        1,
        coalesce((select max(latest_module_no) from progress), 0)
    ) as module_no
),

stage_counts as (
    select
        1 as stage_order,
        null::int as module_no,
        'Started Avni' as stage_short_name,
        count(distinct contact_id) as contact_count
    from progress
    where started_avni_training = 1

    union all

    select
        mn.module_no + 1 as stage_order,
        mn.module_no,
        'Completed Module ' || mn.module_no::text as stage_short_name,
        count(distinct p.contact_id) as contact_count
    from module_numbers mn
    left join progress p
      on p.latest_module_no >= mn.module_no
    group by 1, 2, 3
),

stages_with_dropoff as (
    select
        stage_order,
        module_no,
        stage_short_name,
        contact_count,
        lag(contact_count) over (order by stage_order) as previous_contact_count
    from stage_counts
)

select
    2026 as reporting_year,
    date '2026-03-01' as reporting_year_start,
    date '2027-03-01' as reporting_year_end,
    stage_order,
    module_no,
    stage_short_name,
    case
        when previous_contact_count is null then stage_short_name
        else stage_short_name || ' (dropoff '
            || round(
                ((previous_contact_count - contact_count)::numeric
                    / nullif(previous_contact_count, 0)) * 100,
                1
            )::text
            || '%)'
    end as stage_name,
    contact_count,
    previous_contact_count,
    case
        when previous_contact_count is null then null
        else previous_contact_count - contact_count
    end as dropoff_count,
    case
        when previous_contact_count is null then null
        else (previous_contact_count - contact_count)::numeric
            / nullif(previous_contact_count, 0)
    end as dropoff_rate,
    case
        when previous_contact_count is null then null
        else contact_count::numeric / nullif(previous_contact_count, 0)
    end as retention_rate
from stages_with_dropoff
