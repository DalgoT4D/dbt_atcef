{{ config(
  materialized='table',
  tags=["final","final_gdgs_2023", "gdgs_2023", "gdgs"]
) }}

SELECT 
    m.*,
    a.ngo_name,
    CASE 
        WHEN e.encounter_type = 'Excavating Machine Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ref('machine_gdgs_23')}} AS m 
LEFT JOIN {{ref('encounters_gdgs_2023')}} AS e 
ON m.machine_id = e.subject_id
JOIN {{ref('machine_gdgs_agg_23')}} a 
        ON m.machine_id = a.machine_sub_id