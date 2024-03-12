{{ config(
  materialized='table'
) }}


WITH WorkOrderEncounters AS (
    SELECT 
        subject_id,
        COUNT(*) OVER(PARTITION BY subject_id) AS encounters_count, -- Count of encounters per subject
        encounter_location,
        encounter_type,
        total_silt_carted,
        date_time
    FROM prod.encounter_2022
)

SELECT 
    s.uid,
    s.dam,
    s.district,
    s.state,
    s.taluka,
    s.village,
    CASE 
        WHEN woe.subject_id IS NOT NULL THEN 'Ongoing'
        ELSE 'Not Started'
    END AS project_status,
    woe.encounter_location,
    woe.encounter_type,
    woe.total_silt_carted,
    woe.date_time
FROM prod.subjects_2022 AS s
LEFT JOIN WorkOrderEncounters AS woe ON s.uid = woe.subject_id

