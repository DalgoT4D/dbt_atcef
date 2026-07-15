{{ config(
    materialized='table',
    tags=['glific', 'intermediate', '2026']
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

source_messages as (
    select
        id as message_id,
        contact_phone,
        flow_name,
        flow_uuid,
        status,
        bsp_status,
        cast(nullif(inserted_at, '') as timestamptz) as inserted_at,
        cast(nullif(bq_inserted_at, '') as timestamptz) as bq_inserted_at
    from {{ source('staging_glific', 'messages_stg') }}
),

eligible_messages as (
    select
        sm.message_id,
        sm.contact_phone,
        sm.flow_name,
        sm.flow_uuid,
        coalesce(sm.inserted_at, sm.bq_inserted_at) as message_at
    from source_messages sm
    where coalesce(sm.inserted_at, sm.bq_inserted_at) >= timestamp '2026-03-01'
      and coalesce(sm.inserted_at, sm.bq_inserted_at) < timestamp '2027-03-01'
      and sm.contact_phone is not null
      and sm.contact_phone not like '987654321%'
      and sm.status = 'sent'
      and sm.bsp_status in ('read', 'delivered', 'sent')
),

stage_messages as (
    select
        em.message_id,
        em.contact_phone,
        em.message_at,
        sm.stage_order,
        sm.stage_short_name,
        sm.stage_name,
        sm.avni_action,
        sm.glific_auto_trigger,
        sm.flow_name,
        sm.flow_uuid
    from eligible_messages em
    inner join stage_map sm
      on em.flow_uuid = sm.flow_uuid
)

select
    2026 as reporting_year,
    date '2026-03-01' as reporting_year_start,
    date '2027-03-01' as reporting_year_end,
    contact_phone,
    stage_order,
    stage_short_name,
    stage_name,
    avni_action,
    glific_auto_trigger,
    flow_name,
    flow_uuid,
    min(message_at) as first_message_at,
    min(message_at)::date as first_message_date,
    count(distinct message_id) as outbound_message_count
from stage_messages
group by
    contact_phone,
    stage_order,
    stage_short_name,
    stage_name,
    avni_action,
    glific_auto_trigger,
    flow_name,
    flow_uuid
