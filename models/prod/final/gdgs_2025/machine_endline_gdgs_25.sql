{{ config(
  materialized='table',
  tags=["final","final_gdgs_2025", "gdgs_2025", "gdgs"]
) }}

SELECT
    m.*,
    a.ngo_name,
    CASE
        WHEN e.encounter_type = 'Excavating Machine Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ ref('machine_gdgs_2025') }} AS m
LEFT JOIN {{ ref('encounters_2025') }} AS e
    ON m.machine_id = e.subject_id
INNER JOIN {{ ref('machine_gdgs_agg_25') }} AS a
    ON m.machine_id = a.machine_sub_id
