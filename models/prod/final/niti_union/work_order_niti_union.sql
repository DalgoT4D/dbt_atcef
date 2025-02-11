{{ config(
  materialized='table',
  tags=["final","final_niti_union", "niti"]
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
    area_covered_by_silt,
    number_of_trolleys_carted,
    total_farm_area_silt_is_spread_on,
    total_silt_excavated_by_gp_for_non_farm_purpose,
    total_working_hours_of_machine,
    silt_excavated_as_per_MB_recording,
    approval_status,
    silt_target,
    total_silt_carted,
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
    area_covered_by_silt,
    number_of_trolleys_carted,
    total_farm_area_silt_is_spread_on,
    total_silt_excavated_by_gp_for_non_farm_purpose,
    total_working_hours_of_machine,
    silt_excavated_as_per_MB_recording,
    approval_status,
    silt_target,
    total_silt_carted,
    subject_uid,
    project_ongoing,
    project_not_started,
    project_completed
FROM
    {{ ref('work_order_2023') }}
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
    area_covered_by_silt,
    number_of_trolleys_carted,
    total_farm_area_silt_is_spread_on,
    total_silt_excavated_by_gp_for_non_farm_purpose,
    total_working_hours_of_machine,
    silt_excavated_as_per_MB_recording,
    approval_status,
    silt_target,
    total_silt_carted,
    subject_uid,
    project_ongoing,
    project_not_started,
    project_completed
FROM
    {{ ref('work_order_2024_niti') }}
