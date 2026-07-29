{{ config(
  materialized='table',
  tags=["final", "final_gramin_niti", "gramin_niti", "gramin_25"]
) }}

SELECT
    w.work_order_id,
    e.date_time,
    w.work_order_name,
    w.state,
    w.district,
    w.taluka,
    w.village,
    w.dam,
    w.ngo_name,
    w.silt_target,
    e.total_silt_excavated_by_gp_for_non_farm_purpose
FROM {{ ref('encounters_gramin_25') }} AS e
INNER JOIN {{ ref('work_order_gramin_25') }} AS w
    ON e.subject_id = w.work_order_id
WHERE
    e.encounter_type = 'Gram Panchayat Endline'
    AND e.total_silt_excavated_by_gp_for_non_farm_purpose != 0
    AND w.work_order_voided != TRUE
