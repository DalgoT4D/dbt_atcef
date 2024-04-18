{{ config(
  materialized='table'
) }}


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
FROM 
    {{ ref('subjects_2024') }} AS s
LEFT JOIN 
    {{ ref('encounters_2024') }} AS woe 
ON 
    s.uid = woe.subject_id
