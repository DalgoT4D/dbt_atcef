{{ config(
  materialized='table',
  tags=["final","final_gramin_niti", "gramin_niti"]
) }}

SELECT 
    m.*,
    a.ngo_name,
    CASE 
        WHEN e.encounter_type = 'Excavating Machine Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ref('machine_gramin')}} AS m 
LEFT JOIN {{ref('encounters_gramin')}} AS e 
ON m.machine_id = e.subject_id
JOIN {{ref('machine_gramin_aggregated')}} a
        ON m.machine_id = a.machine_sub_id