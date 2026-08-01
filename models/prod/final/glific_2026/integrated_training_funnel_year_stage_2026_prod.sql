{{ config(
    materialized='table',
    tags=['glific', 'prod', '2026']
) }}

with stage_map as (
    select *
    from (
        values
            (
                1,
                'Login completed',
                'After Login / WO registration training',
                'User logs in to the Avni-Gramin app',
                'User receives training on WO registration',
                'After Login Notification',
                '3c5b24a1-f1a1-4f6d-a1d0-3cecffe2f54b'
            ),
            (
                2,
                'WO registered',
                'After WO registration / Farmer-Machine-GP training',
                'User registers a work order',
                'User receives training on Farmer, Machine, and GP registration',
                'Notification After Successful WO Reg',
                '8251df44-642f-42c4-ba1a-fa2533e4630a'
            ),
            (
                3,
                'Farmer/GP/Machine registered',
                'After Farmer/GP/Machine registration / Daily recording training',
                'User registers Farmer, GP, or Machine records',
                'User receives training on Farmer and Machine Daily Recording',
                'Notification After Farmer Reg',
                '0f514afb-afc5-48eb-909a-da239aef5f11'
            ),
            (
                4,
                'Daily recording or work completion',
                'After daily recording/work completion / Endline forms training',
                'User records Farmer or Machine daily entries, or work completion is detected',
                'User receives training on endline forms',
                'Nudge for endline',
                '9d31f2aa-9c7d-42e2-98f0-ae96ef315852'
            ),
            (
                5,
                'WO endline completed',
                'After WO endline completion / Certificate available',
                'User completes WO endline',
                'User receives training completion certificate',
                'avni_certificate_to_all',
                'bbb7b4f8-387b-47db-8a47-5632233d81dd'
            )
    ) as stages (
        stage_order,
        stage_short_name,
        stage_name,
        avni_action,
        glific_auto_trigger,
        flow_name,
        flow_uuid
    )
),

events as (
    select *
    from {{ ref('integrated_training_trigger_events_2026_int') }}
),

contact_flags as (
    select
        contact_phone,
        max(case when stage_order = 1 then 1 else 0 end) as has_stage_1,
        max(case when stage_order = 2 then 1 else 0 end) as has_stage_2,
        max(case when stage_order = 3 then 1 else 0 end) as has_stage_3,
        max(case when stage_order = 4 then 1 else 0 end) as has_stage_4,
        max(case when stage_order = 5 then 1 else 0 end) as has_stage_5
    from events
    group by 1
),

raw_counts as (
    select
        sm.stage_order,
        count(distinct e.contact_phone) as raw_contact_count,
        min(e.first_message_at) as first_stage_message_at,
        max(e.first_message_at) as latest_stage_message_at
    from stage_map sm
    left join events e
      on sm.stage_order = e.stage_order
    group by 1
),

funnel_counts as (
    select
        1 as stage_order,
        count(*) filter (where has_stage_1 = 1) as funnel_contact_count
    from contact_flags

    union all

    select
        2 as stage_order,
        count(*) filter (where has_stage_1 = 1 and has_stage_2 = 1) as funnel_contact_count
    from contact_flags

    union all

    select
        3 as stage_order,
        count(*) filter (where has_stage_1 = 1 and has_stage_2 = 1 and has_stage_3 = 1) as funnel_contact_count
    from contact_flags

    union all

    select
        4 as stage_order,
        count(*) filter (where has_stage_1 = 1 and has_stage_2 = 1 and has_stage_3 = 1 and has_stage_4 = 1) as funnel_contact_count
    from contact_flags

    union all

    select
        5 as stage_order,
        count(*) filter (
            where has_stage_1 = 1
              and has_stage_2 = 1
              and has_stage_3 = 1
              and has_stage_4 = 1
              and has_stage_5 = 1
        ) as funnel_contact_count
    from contact_flags
),

stage_counts as (
    select
        sm.stage_order,
        sm.stage_short_name,
        sm.stage_name,
        sm.avni_action,
        sm.glific_auto_trigger,
        sm.flow_name,
        sm.flow_uuid,
        coalesce(rc.raw_contact_count, 0) as raw_contact_count,
        coalesce(fc.funnel_contact_count, 0) as funnel_contact_count,
        rc.first_stage_message_at,
        rc.latest_stage_message_at
    from stage_map sm
    left join raw_counts rc
      on sm.stage_order = rc.stage_order
    left join funnel_counts fc
      on sm.stage_order = fc.stage_order
),

stages_with_dropoff as (
    select
        *,
        lag(funnel_contact_count) over (order by stage_order) as previous_funnel_contact_count
    from stage_counts
)

select
    2026 as reporting_year,
    date '2026-03-01' as reporting_year_start,
    date '2027-03-01' as reporting_year_end,
    stage_order,
    stage_short_name,
    stage_name,
    case
        when previous_funnel_contact_count is null then stage_name
        else stage_name || ' (dropoff '
            || round(
                ((previous_funnel_contact_count - funnel_contact_count)::numeric
                    / nullif(previous_funnel_contact_count, 0)) * 100,
                1
            )::text
            || '%)'
    end as stage_name_with_dropoff,
    avni_action,
    glific_auto_trigger,
    flow_name,
    flow_uuid,
    raw_contact_count,
    funnel_contact_count,
    funnel_contact_count as contact_count,
    previous_funnel_contact_count,
    case
        when previous_funnel_contact_count is null then null
        else previous_funnel_contact_count - funnel_contact_count
    end as dropoff_count,
    case
        when previous_funnel_contact_count is null then null
        else (previous_funnel_contact_count - funnel_contact_count)::numeric
            / nullif(previous_funnel_contact_count, 0)
    end as dropoff_rate,
    case
        when previous_funnel_contact_count is null then null
        else funnel_contact_count::numeric / nullif(previous_funnel_contact_count, 0)
    end as retention_rate,
    first_stage_message_at,
    latest_stage_message_at
from stages_with_dropoff
