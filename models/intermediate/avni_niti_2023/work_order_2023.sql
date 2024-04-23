{{ config(
  materialized='table'
) }}


WITH WorkOrderEncounters AS (
    SELECT 
        subject_id,
        eid,
        COUNT(*) OVER(PARTITION BY subject_id) AS encounters_count, -- Count of encounters per subject
        encounter_location,
        encounter_type,
        total_silt_carted,
        date_time,
        working_hours_as_per_time,
        total_working_hours_of_machine_by_time,
        total_working_hours_of_machine,
        silt_excavated_as_per_MB_recording,
        silt_carted_by_farmer_trolleys,
        total_silt_excavated,
        number_of_trolleys_carted,
        total_farm_area_on_which_Silt_is_spread,
        total_silt_excavated_by_GP_for_non_farm_purpose
    FROM {{ ref('encounter_2023') }}
)

SELECT 
    s.uid,
    woe.eid,
    s.first_name,
    s.mobile_verified,
    s.mobile_number,
    s.dam,
    s.ngo_name,
    s.district,
    s.state,
    s.taluka,
    s.village,
    s.type_of_machine,
    s.silt_target,
    woe.working_hours_as_per_time,
    woe.total_working_hours_of_machine_by_time,
    woe.total_working_hours_of_machine,
    woe.silt_excavated_as_per_MB_recording,
    woe.total_silt_excavated,
    woe.total_farm_area_on_which_Silt_is_spread,
    woe.total_silt_excavated_by_GP_for_non_farm_purpose,
    CASE 
        WHEN woe.subject_id IS NOT NULL AND woe.encounter_type <> 'Work order endline' THEN 'Ongoing'
    ELSE NULL
    END AS project_ongoing,
    CASE 
        WHEN woe.subject_id IS NULL THEN 'Yet to start' 
        ELSE NULL
    END AS project_not_started,
    CASE 
        WHEN woe.encounter_type = 'Work order endline' THEN 'Completed'
        ELSE NULL
    END AS project_completed,
    woe.encounter_location,
    woe.encounter_type,
    woe.total_silt_carted,
    woe.date_time,
    s.category_of_farmer,
    woe.silt_carted_by_farmer_trolleys,
    woe.number_of_trolleys_carted
FROM {{ ref('subjects_2023') }} AS s
LEFT JOIN WorkOrderEncounters AS woe ON s.uid = woe.subject_id 
where 
      dam is not null
      and district is not null
      and taluka is not null
      and state is not null
      and village is not null


