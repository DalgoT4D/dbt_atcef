{{ config(
  materialized='table',
  tags=["final","final_niti_2025", "niti_2025", "niti"]
) }}

SELECT
    m.*,
    a.ngo_name,
    CASE
        WHEN e.encounter_type = 'Excavating Machine Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ ref('machine_niti_2025') }} AS m
LEFT JOIN {{ ref('encounters_niti_2025') }} AS e
    ON m.machine_id = e.subject_id
INNER JOIN {{ ref('machine_niti_2025_agg') }} AS a
    ON m.machine_id = a.machine_sub_id
