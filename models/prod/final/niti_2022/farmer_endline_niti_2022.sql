{{ config(
  materialized='table'
) }}

SELECT 
        m.farmer_id,
m.farmer_name,
m.state,
m.taluka,
m.village,
m.dam,
m.district,
    CASE 
        WHEN e.encounter_type = 'Farmer Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ref('farmer_niti_22')}} AS m 
LEFT JOIN {{ref('encounter_2022')}} AS e 
ON m.farmer_id = e.subject_id 