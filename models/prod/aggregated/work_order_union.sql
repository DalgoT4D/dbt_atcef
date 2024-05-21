{{ config(
  materialized='table'
) }}



SELECT
    uid,
    eid,
    date_time,
    ngo_name,
    first_name,
    encounter_type,
    dam,
    district,
    state,
    taluka,
    village,
    category_of_farmer,
    type_of_machine,
    mobile_number,
    mobile_verified,
    total_farm_area,
    area_covered_by_silt,
    number_of_trolleys_carted,
    total_farm_area_on_which_silt_is_spread,
    total_silt_excavated_by_gp_for_non_farm_purpose,
    total_working_hours_of_machine,
    approval_status,
    subject_uid,
    project_ongoing,
    project_not_started,
    project_completed
FROM
    {{ ref('work_order_2022') }} 
UNION ALL
SELECT
    uid,
    eid,
    date_time,
    ngo_name,
    first_name,
    encounter_type,
    dam,
    district,
    state,
    taluka,
    village,
    category_of_farmer,
    type_of_machine,
    mobile_number,
    mobile_verified,
    total_farm_area,
    area_covered_by_silt,
    number_of_trolleys_carted,
    total_farm_area_on_which_silt_is_spread,
    total_silt_excavated_by_gp_for_non_farm_purpose,
    total_working_hours_of_machine,
    approval_status,
    subject_uid,
    project_ongoing,
    project_not_started,
    project_completed
FROM
    {{ ref('work_order_2023') }}
