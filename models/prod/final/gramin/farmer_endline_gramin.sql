{{ config(
  materialized='table'
) }}

SELECT 
    m.*,
    CASE 
        WHEN e.encounter_type = 'Farmer Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ref('farmer_gramin')}} AS m 
LEFT JOIN {{ref('encounters_gramin')}} AS e 
ON m.farmer_id = e.subject_id 