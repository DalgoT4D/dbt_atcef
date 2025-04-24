{{ config(
  materialized='table',
  tags=["final","final_gramin_niti", "gramin_niti", "gramin_25"]
) }}

SELECT
    m.*,
    a.ngo_name,
    CASE
        WHEN e.encounter_type = 'Excavating Machine Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ ref('machine_gramin_25') }} AS m
LEFT JOIN {{ ref('encounters_gramin_25') }} AS e
    ON m.machine_id = e.subject_id
INNER JOIN {{ ref('machine_gramin_aggregated_25') }} AS a
    ON m.machine_id = a.machine_sub_id
