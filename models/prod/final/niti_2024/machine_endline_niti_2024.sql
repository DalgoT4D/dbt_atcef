{{ config(
  materialized='table'
) }}

SELECT 
    m.*,
    CASE 
        WHEN e.encounter_type = 'Excavating Machine Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ref('machine_niti_2024')}} AS m 
LEFT JOIN {{ref('encounters_niti_2024')}} AS e 
ON m.machine_id = e.subject_id