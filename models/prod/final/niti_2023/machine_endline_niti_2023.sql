{{ config(
  materialized='table',
  tags=["final","final_niti_2023", "niti_2023", "niti"]
) }}

SELECT 
    m.*,
    a.ngo_name,
    CASE 
        WHEN e.encounter_type = 'Excavating Machine Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ref('machine_niti_2023')}} AS m 
LEFT JOIN {{ref('encounter_2023')}} AS e 
ON m.machine_id = e.subject_id
JOIN {{ref('machine_niti_2023_agg')}} a
        ON m.machine_id = a.machine_sub_id