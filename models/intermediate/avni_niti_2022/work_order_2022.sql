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
        total_silt_excavated
    FROM {{ ref('encounter_2022') }}
)

SELECT 
    woe.eid,
    s.uid,
    s.first_name,
    s.mobile_verified,
    s.mobile_number,
    s.dam,
    s.district,
    s.state,
    s.taluka,
    s.village,
    s.type_of_machine,
    woe.working_hours_as_per_time,
    woe.total_working_hours_of_machine_by_time,
    woe.total_working_hours_of_machine,
    woe.silt_excavated_as_per_MB_recording,
    woe.total_silt_excavated,
    CASE WHEN woe.subject_id IS NOT NULL THEN 'Ongoing' END AS project_ongoing,
    CASE WHEN woe.subject_id IS NULL  THEN 'Yet to start' END AS project_not_started,
    woe.encounter_location,
    woe.encounter_type,
    s.silt_to_be_excavated,
    woe.total_silt_carted,
    woe.date_time,
    s.category_of_farmer,
    woe.silt_carted_by_farmer_trolleys
FROM {{ ref('subjects_2022') }} AS s
LEFT JOIN WorkOrderEncounters AS woe ON s.uid = woe.subject_id

