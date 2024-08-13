{{ config(
  materialized='table'
) }}


SELECT 
    m.*,
    CASE 
        WHEN e.encounter_type = 'Farmer Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM intermediate_avni_gdgsgom_2024_cleaned.farmer_gdgs_2024 AS m 
LEFT JOIN intermediate_avni_gdgsgom_2024.encounters_2024 AS e 
ON m.farmer_id = e.subject_id 