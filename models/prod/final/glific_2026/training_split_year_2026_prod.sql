{{ config(
  materialized='table',
  tags=['glific', 'prod', '2026']
) }}

with base_training_status as (
    select
        reporting_year,
        reporting_year_start,
        reporting_year_end,
        contact_id,
        case
            when avni_training_completed = 1 and rwb_training_completed = 0 then 'Avni Training Only'
            when avni_training_completed = 0 and rwb_training_completed = 1 then 'RWB Training Only'
            when avni_training_completed = 1 and rwb_training_completed = 1 then 'Both Trainings'
            else 'Training Not Started'
        end as training_type
    from {{ ref('contact_flow_status_year_2026_prod') }}
)

select
    reporting_year,
    reporting_year_start,
    reporting_year_end,
    training_type,
    count(distinct contact_id) as num_people
from base_training_status
group by 1, 2, 3, 4
order by
    case training_type
        when 'Avni Training Only' then 1
        when 'Both Trainings' then 2
        when 'RWB Training Only' then 3
        when 'Training Not Started' then 4
    end
