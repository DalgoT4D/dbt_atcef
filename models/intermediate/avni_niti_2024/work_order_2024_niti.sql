{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2024"]
) }}

WITH WorkOrderEncounters AS (
    SELECT 
        e.*,
        subject_id as subject_uid
    FROM {{ ref('encounters_niti_2024') }} AS e
    LEFT JOIN {{ ref('subjects_niti_2024') }} AS s 
        ON e.subject_id = s.uid 
        AND e.encounter_type <> 'Work order daily Recording - Farmer'
        AND s.subject_voided = false
    LEFT JOIN {{ ref('subjects_niti_2024') }} AS f 
        ON e.farmer_sub_id = f.uid 
        AND e.encounter_type = 'Work order daily Recording - Farmer'
        AND f.subject_voided = false
    WHERE 
        (e.encounter_type = 'Work order daily Recording - Farmer' 
            AND e.farmer_sub_id IS NOT NULL 
            AND e.machine_sub_id IS NOT NULL 
            AND f.uid IS NOT NULL)
        OR (e.encounter_type <> 'Work order daily Recording - Farmer' 
            AND s.uid IS NOT NULL)
)

SELECT 
    s.*,
    woe.*,
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
    END AS project_completed
FROM {{ ref('subjects_niti_2024') }} AS s
LEFT JOIN WorkOrderEncounters AS woe 
    ON s.uid = woe.subject_uid
WHERE 
    s.dam IS NOT NULL
    AND s.district IS NOT NULL
    AND s.taluka IS NOT NULL
    AND s.state IS NOT NULL
    AND s.village IS NOT NULL
    AND s.subject_voided = false

