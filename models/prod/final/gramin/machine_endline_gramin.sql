{{ config(
  materialized='table',
  tags=["final","final_gramin_niti"]
) }}

SELECT 
    m.*,
    CASE 
        WHEN e.encounter_type = 'Excavating Machine Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ref('machine_gramin')}} AS m 
LEFT JOIN {{ref('encounters_gramin')}} AS e 
ON m.machine_id = e.subject_id