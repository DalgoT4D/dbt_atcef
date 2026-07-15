{{ config(
    materialized='table',
    tags=['glific', 'prod', '2026']
) }}

with contact_flows as (
    select
        2026 as reporting_year,
        date '2026-03-01' as reporting_year_start,
        date '2027-03-01' as reporting_year_end,
        c.contact_id,
        c.phone,
        c.inserted_date,
        c.updated_date,
        c.username_lms,
        split_part(c.username_lms, '@', 2) as project,
        coalesce(fcp.avni_gramin_initiated_flow, 0) as avni_gramin_initiated_flow,
        coalesce(fcp.rwb_training_initiated_flow, 0) as rwb_training_initiated_flow,
        coalesce(fcp.completed_both_trainings, 0) as completed_both_trainings
    from {{ ref('contacts_field_flatten_int') }} c
    left join {{ ref('flow_contexts_prod') }} fcp
      on cast(c.contact_id as varchar) = cast(fcp.contact_id as varchar)
    where c.inserted_date >= date '2026-03-01'
      and c.inserted_date < date '2027-03-01'
)

select
    cf.*,
    case
        when cff.is_avni_training_done = 'y' then 1
        else 0
    end as avni_training_completed,
    case
        when cff.is_rwb_training_done = 'y' then 1
        else 0
    end as rwb_training_completed
from contact_flows cf
left join {{ ref('contacts_field_flatten_int') }} cff
  on cf.contact_id = cff.contact_id
