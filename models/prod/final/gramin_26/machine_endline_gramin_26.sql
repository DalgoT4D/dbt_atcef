{{ config(
  materialized='table',
  tags=["final","final_gramin_niti", "gramin_niti", "gramin_26"]
) }}

SELECT
    m.machine_id,
    m.subject_type,
    m.machine_voided,
    m.machine_name,
    m.type_of_machine,
    m.dam,
    m.district,
    m.state,
    m.taluka,
    m.village,
    m.machine_approval_status,
    a.ngo_name,
    CASE
        WHEN e.encounter_type = 'Excavating Machine Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ ref('machine_gramin_26') }} AS m
LEFT JOIN {{ ref('encounters_gramin_26') }} AS e
    ON m.machine_id = e.subject_id
INNER JOIN {{ ref('machine_gramin_aggregated_26') }} AS a
    ON m.machine_id = a.machine_sub_id
