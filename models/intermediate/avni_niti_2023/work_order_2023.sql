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
        machine_working_hours
    FROM {{ ref('encounter_2023') }}
)

SELECT 
    s.uid,
    woe.eid,
    s.mobile_verified,
    s.dam,
    s.district,
    s.state,
    s.taluka,
    s.village,
    s.type_of_machine,
    woe.machine_working_hours,
    CASE 
        WHEN woe.subject_id IS NOT NULL THEN 'Ongoing'
        ELSE 'Not Started'
    END AS project_status,
    woe.encounter_location,
    woe.encounter_type,
    s.silt_to_be_excavated,
    woe.total_silt_carted,
    woe.date_time
FROM {{ ref('subjects_2023') }} AS s
LEFT JOIN WorkOrderEncounters AS woe ON s.uid = woe.subject_id

