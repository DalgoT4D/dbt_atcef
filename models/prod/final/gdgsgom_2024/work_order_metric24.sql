{{ config(
  materialized='table'
) }}


SELECT 
    first_name as work_order,
    MAX(date_time) as date_time,
    state, 
    district,
    taluka,
    dam,
    village,
    max(silt_to_be_excavated_as_per_plan) as silt_target,
    sum(total_silt_carted) as silt_achieved
FROM   {{ref('work_order_2024')}}
WHERE
    encounter_type = 'Work order daily Recording - Farmer'
    and project_ongoing = 'Ongoing'
GROUP BY
    state, district, taluka, dam, village, date_time, first_name