{{ config(
  materialized='table'
) }}

with silt_data as (
    select
        dam,
        district,
        state,
        taluka,
        village,
        project_status,
        encounter_location,
        sum(silt_to_be_excavated) as silt_target_per_dam, -- sum of silt_to_be_excavated per dam
        sum(total_silt_carted) as silt_excavated_per_dam, -- sum of total_silt_carted per dam
        max(date_time) as latest_encounter_date -- latest date of silt excavation
    from {{ ref('work_order_union') }} 
    where encounter_type = 'Work order daily Recording - Farmer'
    group by dam, district, state, taluka, village, project_status, encounter_location
),

progress as (
    select
        dam,
        district,
        state,
        taluka,
        village,
        project_status,
        encounter_location,
        silt_target_per_dam,
        silt_excavated_per_dam,
        (silt_excavated_per_dam / nullif(silt_target_per_dam, 0)) * 100 as excavation_progress_percentage,
        latest_encounter_date
    from silt_data
)

select * from progress
